package bot

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/rkfg/guiltyspark/config"
	"github.com/rkfg/guiltyspark/embedding"
	"github.com/rkfg/guiltyspark/indexer"
	"github.com/rkfg/guiltyspark/retry"
	"github.com/rkfg/guiltyspark/search"
)

type Bot struct {
	cfg            *config.Config
	client         *mautrix.Client
	embedClient    *embedding.Client
	bleveClient    *indexer.BleveClient
	batchIndexer   *indexer.BatchIndexer
	imageProcessor *indexer.ImageProcessor
	commandHandler *CommandHandler
	searchEngine   *search.Engine
	startTime      time.Time
	gracePeriod    time.Duration
	joinedInviteRooms map[string]bool // Track rooms we already tried to join
}

func New(cfg *config.Config) (*Bot, error) {
	// Create embedding client
	embedClient := embedding.NewClient(&cfg.LLM, retry.BackoffConfig{
		InitialDelay: cfg.Retry.InitialDelay,
		MaxDelay:     cfg.Retry.MaxDelay,
		Multiplier:   cfg.Retry.Multiplier,
		MaxRetries:   cfg.Retry.MaxRetries,
	}, cfg.Retry.Timeout)

	// Create Bleve client
	bleveClient, err := indexer.NewBleveClient(
		fmt.Sprintf("%s/index.bleve", cfg.StoragePath),
		cfg.Search.VectorDimensions,
	)
	if err != nil {
		return nil, fmt.Errorf("create bleve client: %w", err)
	}

	// Create batch indexer
	batchIndexer := indexer.NewBatchIndexer(cfg.Indexing.DelayedEmbedHour, cfg.Indexing.DelayedEmbedMinute)

	// Create image processor
	imageProcessor := indexer.NewImageProcessor(&cfg.ImageProc, embedClient)
	imageProcessor.SetHomeserver(cfg.Bot.Homeserver, cfg.Bot.AccessToken)

	// Create search engine
	searchEngine := search.NewEngine(bleveClient, embedClient, &cfg.Search)

	// Create command handler
	commandHandler := NewCommandHandler(searchEngine)

	// Store grace period for filtering old messages
	gracePeriod := cfg.Indexing.StartupGracePeriod

	// Set up batch indexer callbacks
	batchIndexer.IndexTextFn = func(doc indexer.IndexedDocument) error {
		// Index in Bleve for text search
		// Use struct-based indexing to preserve []float32 type for vector field
		// map[string]interface{} would convert []float32 to []interface{}{float64, ...}
		if err := bleveClient.IndexDocumentStruct(doc); err != nil {
			return err
		}
		return nil
	}
	batchIndexer.IsIndexedFn = func(eventID string) (bool, error) {
		return bleveClient.IsEventIndexed(eventID)
	}
	batchIndexer.ProcessDeferredFn = func(images []indexer.PendingImage) ([]indexer.PendingImage, error) {
		var failed []indexer.PendingImage
		for _, img := range images {
			result, err := imageProcessor.ProcessImage(img.RawURL, img.RoomID, img.UserID, img.EventID, img.Timestamp, img.RawURL, img.FileName, img.MimeType)
			if err != nil {
				log.Printf("Failed to process deferred image %s: %v", img.EventID, err)
				failed = append(failed, img)
				continue
			}

			doc := indexer.IndexedDocument{
				ID:        fmt.Sprintf("%s:%s", img.RoomID, img.EventID),
				EventID:   img.EventID,
				RoomID:    img.RoomID,
				UserID:    img.UserID,
				Timestamp: img.Timestamp,
				EventType: "m.room.message",
				ImageDesc: result.Description,
				Vector:    result.Vector,
				RawURL:    img.RawURL,
				FileName:  img.FileName,
				MimeType:  img.MimeType,
			}

			if err := bleveClient.IndexDocumentStruct(doc); err != nil {
				log.Printf("Failed to index deferred image %s: %v", img.EventID, err)
				failed = append(failed, img)
				continue
			}
		}
		return failed, nil
	}
	batchIndexer.ProcessDeferredTextFn = func(texts []indexer.PendingMessage) ([]indexer.PendingMessage, error) {
		var failed []indexer.PendingMessage
		for _, msg := range texts {
			vector, err := embedClient.CreateEmbedding(msg.Text)
			if err != nil {
				log.Printf("Failed to embed deferred text %s: %v", msg.EventID, err)
				failed = append(failed, msg)
				continue
			}

			doc := indexer.IndexedDocument{
				ID:        fmt.Sprintf("%s:%s", msg.RoomID, msg.EventID),
				EventID:   msg.EventID,
				RoomID:    msg.RoomID,
				UserID:    msg.UserID,
				Timestamp: msg.Timestamp,
				EventType: msg.EventType,
				Text:      msg.Text,
				Vector:    vector,
			}

			if err := bleveClient.IndexDocumentStruct(doc); err != nil {
				log.Printf("Failed to index deferred text %s: %v", msg.EventID, err)
				failed = append(failed, msg)
				continue
			}
		}
		return failed, nil
	}

	return &Bot{
		cfg:             cfg,
		embedClient:     embedClient,
		bleveClient:     bleveClient,
		batchIndexer:    batchIndexer,
		imageProcessor:  imageProcessor,
		commandHandler:  commandHandler,
		searchEngine:    searchEngine,
		gracePeriod:     gracePeriod,
		joinedInviteRooms: make(map[string]bool),
	}, nil
}

func (b *Bot) Start() error {
	// Parse server name from homeserver URL
	parsedServer := id.ParseServerName(b.cfg.Bot.Homeserver)
	if parsedServer == nil {
		return fmt.Errorf("invalid homeserver URL: %s", b.cfg.Bot.Homeserver)
	}

	// Create mautrix client
	userID := id.UserID(b.cfg.Bot.UserID)
	var err error
	b.client, err = mautrix.NewClient(b.cfg.Bot.Homeserver, userID, b.cfg.Bot.AccessToken)
	if err != nil {
		return fmt.Errorf("create mautrix client: %w", err)
	}

	// Set state store and syncer
	b.client.StateStore = mautrix.NewMemoryStateStore()
	b.client.Syncer = mautrix.NewDefaultSyncer()

	// Set up event handlers
	syncer := b.client.Syncer.(*mautrix.DefaultSyncer)
	syncer.OnEventType(event.EventMessage, func(ctx context.Context, ev *event.Event) {
		b.handleEvent(ev)
	})
	// Auto-join when invited to a room (DM or otherwise)
	syncer.OnEventType(event.StateMember, func(ctx context.Context, ev *event.Event) {
		b.handleMembershipEvent(ev)
	})
	// Handle invite events from sync response
	syncer.OnSync(b.handleSyncResponse)

	// Populate state store with current joined rooms
	if err := b.populateStateStore(); err != nil {
		log.Printf("Failed to populate state store: %v", err)
	}

	// Record start time — only index messages sent after startTime + gracePeriod
	b.startTime = time.Now().Add(-b.gracePeriod)

	// Start syncing
	go func() {
		err := b.client.SyncWithContext(context.Background())
		if err != nil {
			log.Printf("Sync error: %v", err)
		}
	}()

	log.Printf("Bot connected as %s (start time: %s)", b.cfg.Bot.UserID, b.startTime.Format(time.RFC3339))
	return nil
}

// isDirectMessage checks if the event is from a DM (1:1 chat).
// Commands are only processed if the message is from a direct room.
func (b *Bot) isDirectMessage(ev *event.Event) bool {
	// First, check m.direct account data
	directRooms, err := b.getDirectRooms()
	if err == nil && len(directRooms) > 0 {
		// If m.direct is set, check if this room is in any user's direct rooms
		for _, rooms := range directRooms {
			for _, room := range rooms {
				if room == ev.RoomID {
					return true
				}
			}
		}
		return false
	}
	
	// If m.direct is not available, check the room joined_members count
	// A DM should have exactly 2 members (user + bot)
	resp, err := b.client.JoinedMembers(context.Background(), ev.RoomID)
	if err != nil {
		log.Printf("Failed to get joined members for %s: %v", ev.RoomID, err)
		return false // Default to false on error — block commands
	}
	
	// Also check if the room has isDirect flag set (Matrix spec)
	// This helps for newly created DMs where m.direct might not be synced yet
	if len(resp.Joined) < 3 {
		return true
	}
	return false
}

// getDirectRooms returns the map of room IDs that are marked as direct in m.direct account data.
func (b *Bot) getDirectRooms() (map[id.UserID][]id.RoomID, error) {
	var directRooms event.DirectChatsEventContent
	err := b.client.GetAccountData(context.Background(), event.AccountDataDirectChats.Type, &directRooms)
	if err != nil {
		return nil, err
	}
	return directRooms, nil
}

// roomAliasRegex matches room aliases like #alias:server and room event IDs like !event:server.
var roomAliasRegex = regexp.MustCompile(`(?:https?://matrix\.to/#/|#|!)([\w-]+):([\w.]+)`)

// userAliasRegex matches user IDs like @user:server.
var userAliasRegex = regexp.MustCompile(`(?:https?://matrix\.to/#/@|@)([\w.]+):([\w.]+)`)
	
// UserRef contains a resolved user ID and the cleaned text.
type UserRef struct {
	UserID string
	Text   string
}

// resolveUserFromText extracts user IDs from text and returns the resolved user ID and cleaned text.
func resolveUserFromText(text string) (string, string) {
	matches := userAliasRegex.FindAllStringSubmatchIndex(text, -1)
	if len(matches) == 0 {
		return "", text
	}

	// Use the first match
	match := matches[0]
	userID := "@" + text[match[2]:match[3]] + ":" + text[match[4]:match[5]]
	
	// Remove the user reference from text
	cleaned := text[:match[0]] + text[match[1]:]
	cleaned = strings.TrimSpace(cleaned)
	
	return userID, cleaned
}

// resolveRoomFromText extracts room aliases/IDs from text, resolves them to room IDs,
// and returns the resolved room ID and the cleaned text without the room reference.
func (b *Bot) resolveRoomFromText(text string) (string, string) {
	// Find all room references in text
	matches := roomAliasRegex.FindAllStringSubmatch(text, -1)
	if len(matches) == 0 {
		return "", text
	}

	// Use the first match
	match := matches[0]
	roomIDOrAlias := match[0] // Full match like "#s:example.org" or "!roomid:example.com"

	// Determine if it's an alias (starts with #) or room ID (starts with !)
	if strings.HasPrefix(roomIDOrAlias, "#") {
		// It's a room alias — resolve it to a room ID
		alias := id.RoomAlias("#" + match[1] + ":" + match[2])
		roomID, err := b.resolveAliasToRoomID(alias)
		if err != nil {
			log.Printf("Failed to resolve alias %s: %v", alias, err)
			return "", text
		}
		// Remove the room reference from text
		cleaned := strings.Replace(text, roomIDOrAlias, "", 1)
		cleaned = strings.TrimSpace(cleaned)
		return roomID.String(), cleaned
	} else if strings.HasPrefix(roomIDOrAlias, "!") {
		// It's a room ID — use it directly (e.g., !roomid:example.com)
		roomID := id.RoomID("!" + match[1] + ":" + match[2])
		cleaned := strings.Replace(text, roomIDOrAlias, "", 1)
		cleaned = strings.TrimSpace(cleaned)
		return roomID.String(), cleaned
	}

	return "", text
}

// parseRoomAndUserFromHTML extracts room alias and user ID from HTML body.
// Returns resolved room ID, resolved user ID, and cleaned HTML without the links.
func (b *Bot) parseRoomAndUserFromHTML(html string) (string, string, string) {
	// Extract room aliases from full <a href="https://matrix.to/#/#room:server">...</a> links
	roomLinkRegex := regexp.MustCompile(`<a[^>]+href="https?://matrix\.to/#/(#[^"]+)"[^>]*>.*?</a>`)
	userLinkRegex := regexp.MustCompile(`<a[^>]+href="https?://matrix\.to/#/(@[^"]+)"[^>]*>.*?</a>`)
	
	var roomID string
	var userID string
	cleanedHTML := html
	
	// Extract room from HTML links
	roomMatches := roomLinkRegex.FindAllStringSubmatch(html, -1)
	if len(roomMatches) > 0 {
		alias := roomMatches[0][1] // Full alias like #room:server
		if strings.HasPrefix(alias, "#") {
			aliasID := id.RoomAlias(alias)
			resolved, err := b.resolveAliasToRoomID(aliasID)
			if err == nil {
				roomID = resolved.String()
				// Remove the full link tag from HTML
				cleanedHTML = roomLinkRegex.ReplaceAllString(cleanedHTML, "")
			}
		}
	}
	
	// Extract user from HTML links
	userMatches := userLinkRegex.FindAllStringSubmatch(html, -1)
	if len(userMatches) > 0 {
		userID = userMatches[0][1] // Full user ID like @user:server
		// Remove the full link tag from HTML
		cleanedHTML = userLinkRegex.ReplaceAllString(cleanedHTML, "")
	}
	
	cleanedHTML = strings.TrimSpace(cleanedHTML)
	
	return roomID, userID, cleanedHTML
}

// resolveAliasToRoomID resolves a room alias to a room ID using the Matrix API.
func (b *Bot) resolveAliasToRoomID(alias id.RoomAlias) (id.RoomID, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := b.client.ResolveAlias(ctx, alias)
	if err != nil {
		return "", fmt.Errorf("resolve room alias: %w", err)
	}

	return id.RoomID(resp.RoomID), nil
}


func (b *Bot) handleEvent(ev *event.Event) {
	// Skip messages from the bot itself
	if ev.Sender.String() == b.cfg.Bot.UserID {
		return
	}

	body, ok := ev.Content.Parsed.(*event.MessageEventContent)
	if !ok {
		return
	}

	// Check if it's a command (text message)
	if body.MsgType == event.MsgText && len(body.Body) > 0 {
		if body.Body[0] == '!' {
			cmd, args, isCommand := ResolveCommand(body.Body)
			if !isCommand {
				// Unknown command — reply with help only if message is from after bot started
				evTime := time.UnixMilli(ev.Timestamp)
				if evTime.After(b.startTime) {
					b.sendReply(ev.RoomID, ev.ID, fmt.Sprintf("Unknown command: %s\n%s", body.Body, HelpText()))
				}
				return
			}
			// Only process commands from messages sent after bot started
			evTime := time.UnixMilli(ev.Timestamp)
			if evTime.After(b.startTime) {
				// Commands are only processed in DMs
				if !b.isDirectMessage(ev) {
					return
				}

				// Try to resolve room and user from formatted_body (HTML)
				searchRoomID := ""
				searchUser := ""
				
				formattedBody := ""
				if body.Format == event.FormatHTML {
					formattedBody = body.FormattedBody
				}
				
				if formattedBody != "" {
					// Extract room and user from HTML
					searchRoomID, searchUser, formattedBody = b.parseRoomAndUserFromHTML(formattedBody)
					args = formattedBody
				} else {
					// Fall back to body text
					if resolvedRoomID, cleanedText := b.resolveRoomFromText(args); resolvedRoomID != "" {
						searchRoomID = resolvedRoomID
						args = cleanedText
					}
					if resolvedUserID, cleanedText := resolveUserFromText(args); resolvedUserID != "" {
						searchUser = resolvedUserID
						args = cleanedText
					}
				}

				switch cmd {
				case "search":
					textResult, htmlResult, err := b.commandHandler.HandleSearch(args, searchRoomID, searchUser)
					if err != nil {
						textResult = fmt.Sprintf("Error: %v", err)
						htmlResult = textResult
					}
					b.sendReplyHTML(ev.RoomID, ev.ID, textResult, htmlResult)
				case "search-semantic":
					textResult, htmlResult, err := b.commandHandler.HandleSemanticSearch(args, searchRoomID, searchUser)
					if err != nil {
						textResult = fmt.Sprintf("Error: %v", err)
						htmlResult = textResult
					}
					b.sendReplyHTML(ev.RoomID, ev.ID, textResult, htmlResult)
				case "help":
					b.sendReply(ev.RoomID, ev.ID, HelpText())
				case "stats":
					result, err := b.commandHandler.HandleStats(b.bleveClient)
					if err != nil {
						result = fmt.Sprintf("Error: %v", err)
					}
					b.sendReply(ev.RoomID, ev.ID, result)
				default:
					b.sendReply(ev.RoomID, ev.ID, fmt.Sprintf("Unknown command: %s\n%s", body.Body, HelpText()))
				}
			}
			// Don't index commands — return early
			return
		}

		// Index as text message (non-command text)
		text := body.Body

		// Add link previews if enabled
		if b.cfg.LinkPreview.Enabled {
			urls := extractURLs(text, b.cfg.LinkPreview.MaxURLs)
			if len(urls) > 0 {
				ctx, cancel := context.WithTimeout(context.Background(), b.cfg.LinkPreview.Timeout)
				defer cancel()

				var wg sync.WaitGroup
				mu := sync.Mutex{}
				var previews []*event.LinkPreview

				for _, u := range urls {
					wg.Add(1)
					go func(rawURL string) {
						defer wg.Done()
						preview, err := b.client.GetURLPreview(ctx, rawURL)
						if err != nil {
							log.Printf("Failed to get preview for %s: %v", rawURL, err)
							return
						}
						mu.Lock()
						previews = append(previews, preview)
						mu.Unlock()
					}(u)
				}

				wg.Wait()
				text = buildTextWithPreviews(text, previews)
			}
		}

		msg := indexer.PendingMessage{
			EventID:   ev.ID.String(),
			RoomID:    ev.RoomID.String(),
			UserID:    ev.Sender.String(),
			Timestamp: ev.Timestamp,
			EventType: "m.room.message",
			Text:      text,
		}

		b.batchIndexer.OnTextMessage(msg)
	} else if body.MsgType == event.MsgImage {
		// Index as image message
		img := indexer.PendingImage{
			EventID:   ev.ID.String(),
			RoomID:    ev.RoomID.String(),
			UserID:    ev.Sender.String(),
			Timestamp: ev.Timestamp,
			RawURL:    string(body.URL),
			FileName:  body.FileName,
			MimeType:  body.Info.MimeType,
		}

		b.batchIndexer.OnImageMessage(img)
	}
}

var urlRegex = regexp.MustCompile(`https?://[^\s<>"\)\]}]+`)

// skipPreviewHosts — домены, для которых не нужно запрашивать link preview.
var skipPreviewHosts = map[string]bool{
	"matrix.to": true,
}

// extractURLs извлекает уникальные URL из текста, возвращает не больше maxURLs.
// Исключает ссылки на matrix.to и другие из skipPreviewHosts.
func extractURLs(text string, maxURLs int) []string {
	matches := urlRegex.FindAllString(text, -1)
	seen := make(map[string]bool)
	var urls []string
	for _, m := range matches {
		// Clean up trailing punctuation
		m = strings.TrimRight(m, ".,;:!?)]}")
		if seen[m] {
			continue
		}
		// Skip matrix.to and similar links (they are room aliases, not web pages)
		if shouldSkipPreview(m) {
			continue
		}
		urls = append(urls, m)
		if len(urls) >= maxURLs {
			break
		}
	}
	return urls
}

// shouldSkipPreview проверяет, нужно ли пропустить запрос превью для этого URL.
func shouldSkipPreview(url string) bool {
	for host := range skipPreviewHosts {
		if strings.Contains(url, host) {
			return true
		}
	}
	return false
}

// buildTextWithPreviews добавляет текст превью к сообщению.
func buildTextWithPreviews(text string, previews []*event.LinkPreview) string {
	if len(previews) == 0 {
		return text
	}

	var sb strings.Builder
	sb.WriteString(text)

	for _, p := range previews {
		fmt.Fprintf(&sb, "\npreview: [%s]", p.Title)
		if p.Description != "" {
			sb.WriteString(" - ")
			sb.WriteString(p.Description)
		}
	}

	return sb.String()
}

func (b *Bot) sendReply(roomID id.RoomID, parentEventID id.EventID, text string) {
	content := event.MessageEventContent{
		MsgType: event.MsgText,
		Body:    text,
	}

	if parentEventID != "" {
		content.RelatesTo = &event.RelatesTo{
			InReplyTo: &event.InReplyTo{
				EventID: parentEventID,
			},
		}
	}

	_, err := b.client.SendMessageEvent(context.Background(), roomID, event.EventMessage, content)
	if err != nil {
		log.Printf("Failed to send reply: %v", err)
	}
}

func (b *Bot) sendReplyHTML(roomID id.RoomID, parentEventID id.EventID, text, html string) {
	content := &event.MessageEventContent{
		MsgType:       event.MsgText,
		Body:          text,
		Format:        event.FormatHTML,
		FormattedBody: html,
	}

	if parentEventID != "" {
		content.RelatesTo = &event.RelatesTo{
			InReplyTo: &event.InReplyTo{
				EventID: parentEventID,
			},
		}
	}

	_, err := b.client.SendMessageEvent(context.Background(), roomID, event.EventMessage, content)
	if err != nil {
		log.Printf("Failed to send HTML reply: %v", err)
	}
}

// handleMembershipEvent handles m.room.member state events (joins, invites, kicks, etc.)
// Auto-joins the bot when it receives an invite.
func (b *Bot) handleMembershipEvent(ev *event.Event) {
	content := ev.Content.Parsed.(*event.MemberEventContent)
	
	// Only handle invites to the bot itself
	if content.Membership != event.MembershipInvite || ev.StateKey != &b.cfg.Bot.UserID {
		return
	}
	
	log.Printf("Bot invited to room %s by %s, joining...", ev.RoomID, *ev.StateKey)
	
	// Join the room
	resp, err := b.client.JoinRoom(context.Background(), ev.RoomID.String(), nil)
	if err != nil {
		log.Printf("Failed to join room %s: %v", ev.RoomID, err)
		return
	}
	
	log.Printf("Bot joined room %s (new room ID: %s)", ev.RoomID, resp.RoomID)
}

// handleSyncResponse processes sync responses and auto-joins invited rooms,
// auto-leaves rooms where the bot is the only member.
func (b *Bot) handleSyncResponse(ctx context.Context, resp *mautrix.RespSync, since string) bool {
	// Auto-join invited rooms (skip already processed)
	for roomID := range resp.Rooms.Invite {
		// Skip if we already tried to join this room
		if b.joinedInviteRooms[roomID.String()] {
			continue
		}
		
		log.Printf("Bot has invite to room %s, joining...", roomID)
		
		// For remote rooms, provide the server via Via
		var req *mautrix.ReqJoinRoom
		if strings.Contains(roomID.String(), ":") {
			parts := strings.SplitN(roomID.String(), ":", 2)
			if len(parts) == 2 {
				req = &mautrix.ReqJoinRoom{
					Reason: "Auto-joining via bot sync handler",
					Via: []string{parts[1]},
				}
			}
		}
		
		joinResp, err := b.client.JoinRoom(ctx, roomID.String(), req)
		if err != nil {
			log.Printf("Failed to join room %s: %v", roomID, err)
			// Mark as processed even on failure to avoid infinite retries
			b.joinedInviteRooms[roomID.String()] = true
			continue
		}
		
		b.joinedInviteRooms[roomID.String()] = true
		log.Printf("Joined room %s (new room ID: %s)", roomID, joinResp.RoomID)
	}
	
	// Auto-leave rooms where the bot is the only member
	// Only check rooms in resp.Rooms.Join — resp.Rooms.Leave contains rooms the bot already left
	for roomID := range resp.Rooms.Join {
		membersResp, err := b.client.JoinedMembers(ctx, roomID)
		if err != nil {
			// Skip rooms where we don't have permission (e.g., not a member yet)
			log.Printf("Failed to get joined members for room %s: %v", roomID, err)
			continue
		}
		
		if len(membersResp.Joined) == 1 {
			// Only the bot is in the room — leave it
			log.Printf("Bot is the only member in room %s, leaving...", roomID)
			_, err := b.client.LeaveRoom(ctx, roomID)
			if err != nil {
				log.Printf("Failed to leave room %s: %v", roomID, err)
			} else {
				log.Printf("Left room %s", roomID)
			}
		}
	}
	
	return true
}

// populateStateStore fetches the list of joined rooms and populates the state store.
func (b *Bot) populateStateStore() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := b.client.JoinedRooms(ctx)
	if err != nil {
		return fmt.Errorf("get joined rooms: %w", err)
	}

	log.Printf("Bot is in %d rooms, populating state store...", len(resp.JoinedRooms))

	for _, roomID := range resp.JoinedRooms {
		// Set bot's membership to join
		if err := b.client.StateStore.SetMembership(ctx, id.RoomID(roomID), b.client.UserID, event.MembershipJoin); err != nil {
			log.Printf("Failed to set membership for room %s: %v", roomID, err)
			continue
		}

		// Fetch joined members and populate state store
		membersResp, err := b.client.JoinedMembers(ctx, id.RoomID(roomID))
		if err != nil {
			log.Printf("Failed to get joined members for room %s: %v", roomID, err)
			continue
		}

		for userID := range membersResp.Joined {
			if err := b.client.StateStore.SetMembership(ctx, id.RoomID(roomID), userID, event.MembershipJoin); err != nil {
				log.Printf("Failed to set member %s in room %s: %v", userID, roomID, err)
			}
		}
	}

	return nil
}

func (b *Bot) Stop() {
	b.batchIndexer.Stop()
	b.bleveClient.Close()
	b.client.StopSync()
}
