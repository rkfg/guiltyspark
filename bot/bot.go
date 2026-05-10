package bot

import (
	"slices"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/rs/zerolog"
	"regexp"
	"strings"
	"sync"
	"time"

	"go.mau.fi/util/dbutil"
	_ "go.mau.fi/util/dbutil/litestream"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/crypto/cryptohelper"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/rkfg/guiltyspark/config"
	"github.com/rkfg/guiltyspark/embedding"
	"github.com/rkfg/guiltyspark/indexer"
	"github.com/rkfg/guiltyspark/retry"
	"github.com/rkfg/guiltyspark/search"
)

type Bot struct {
	cfg               *config.Config
	client            *mautrix.Client
	embedClient       *embedding.Client
	bleveClient       *indexer.BleveClient
	batchIndexer      *indexer.BatchIndexer
	imageProcessor    *indexer.ImageProcessor
	commandHandler    *CommandHandler
	searchEngine      *search.Engine
	startTime         time.Time
	gracePeriod       time.Duration
	joinedInviteRooms map[string]bool
	inviteMu          sync.Mutex
	cryptoHelper      *cryptohelper.CryptoHelper
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
	batchIndexer := indexer.NewBatchIndexer(cfg.Indexing.DelayedEmbedHour, cfg.Indexing.DelayedEmbedMinute, cfg.StoragePath)

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
		// map[string]any would convert []float32 to []any{float64, ...}
		if err := bleveClient.IndexDocumentStruct(doc); err != nil {
			return err
		}
		// Mark event ID as processed immediately — prevents re-indexing on restart
		// even if the event was appended to another document (reindex via buffering)
		if err := bleveClient.AddEventID(doc.EventID); err != nil {
			log.Printf("Failed to mark event ID %s as processed: %v", doc.EventID, err)
		}
		return nil
	}
	batchIndexer.IsIndexedFn = func(eventID string) (bool, error) {
		return bleveClient.IsEventIDExists(eventID)
	}
	batchIndexer.AddEventIDFn = func(eventID string) error {
		return bleveClient.AddEventID(eventID)
	}
	batchIndexer.ProcessImageDescFn = func(images []indexer.PendingImage) ([]indexer.PendingImage, error) {
		var failed []indexer.PendingImage
		for i, img := range images {
			desc, err := imageProcessor.DescribeImageOnly(img.RawURL, img.Timestamp)
			if err != nil {
				log.Printf("Failed to describe deferred image %s: %v", img.EventID, err)
				failed = append(failed, img)
				continue
			}
			images[i].Description = desc
		}
		return images, nil
	}
	batchIndexer.ProcessImageEmbedFn = func(images []indexer.PendingImage) ([]indexer.PendingImage, error) {
		var failed []indexer.PendingImage
		for _, img := range images {
			if img.Description == "" {
				failed = append(failed, img)
				continue
			}

			vector, err := embedClient.CreateEmbedding(img.Description)
			if err != nil {
				log.Printf("Failed to embed deferred image %s: %v", img.EventID, err)
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
				ImageDesc: img.Description,
				Vector:    vector,
				RawURL:    img.RawURL,
				FileName:  img.FileName,
				MimeType:  img.MimeType,
			}

			if err := bleveClient.IndexDocumentStruct(doc); err != nil {
				log.Printf("Failed to index deferred image %s: %v", img.EventID, err)
				failed = append(failed, img)
				continue
			}
			if err := bleveClient.AddEventID(img.EventID); err != nil {
				log.Printf("Failed to mark event ID %s as processed: %v", img.EventID, err)
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
			if err := bleveClient.AddEventID(msg.EventID); err != nil {
				log.Printf("Failed to mark event ID %s as processed: %v", msg.EventID, err)
			}
		}
		return failed, nil
	}

	// Create E2EE crypto infrastructure
	// Use INFO level to suppress crypto module debug noise but keep important startup messages
	clientLog := zerolog.New(os.Stdout).With().Timestamp().Logger().Level(zerolog.InfoLevel)
	userID := id.UserID(cfg.Bot.UserID)

	var mclient *mautrix.Client
	var cryptoHelper *cryptohelper.CryptoHelper

	if cfg.E2EE.LoginPassword != "" {
		// New device approach: login with password to create a device with proper Olm account.
		// Clear the Olm account only if there's none — otherwise reuse the existing account
		// across restarts to avoid creating new Olm keys and accumulating sessions.
		if _, err := os.Stat(cfg.E2EE.DBPath); err == nil {
			db, err := dbutil.NewWithDialect(
				fmt.Sprintf("file:%s?_txlock=immediate", cfg.E2EE.DBPath),
				"sqlite3-fk-wal",
			)
			if err != nil {
				return nil, fmt.Errorf("open e2ee database: %w", err)
			}

			var accountCount int
			err = db.QueryRow(context.Background(), "SELECT COUNT(*) FROM crypto_account").Scan(&accountCount)
			if err != nil {
				db.Close()
				return nil, fmt.Errorf("check Olm account: %w", err)
			}

			if accountCount == 0 {
				log.Printf("No existing Olm account, creating fresh E2EE device")
				_, err = db.Exec(context.Background(), "DELETE FROM crypto_account")
				if err != nil {
					db.Close()
					return nil, fmt.Errorf("clear Olm account: %w", err)
				}
			} else {
				log.Printf("Existing Olm account found (%d), reusing", accountCount)
			}
			db.Close()
		}

		mclient, err = mautrix.NewClient(cfg.Bot.Homeserver, userID, "")
		if err != nil {
			return nil, fmt.Errorf("create mautrix client: %w", err)
		}
		mclient.Log = clientLog

		cryptoHelper, err = cryptohelper.NewCryptoHelper(mclient, []byte(cfg.E2EE.PickleKey), cfg.E2EE.DBPath)
		if err != nil {
			return nil, fmt.Errorf("create crypto helper: %w", err)
		}
		cryptoHelper.DBAccountID = "guiltyspark-bot"
		cryptoHelper.LoginAs = &mautrix.ReqLogin{
			Type:                   mautrix.AuthTypePassword,
			Identifier:             mautrix.UserIdentifier{Type: mautrix.IdentifierTypeUser, User: cfg.Bot.UserID},
			Password:               cfg.E2EE.LoginPassword,
			InitialDeviceDisplayName: "guiltyspark-search-bot",
		}
	} else {
		// Fallback: use existing access token (E2EE may not work properly)
		log.Println("E2EE: using existing access token (E2EE may not work in encrypted rooms)")
		mclient, err = mautrix.NewClient(cfg.Bot.Homeserver, userID, cfg.Bot.AccessToken)
		if err != nil {
			return nil, fmt.Errorf("create mautrix client: %w", err)
		}
		mclient.Log = clientLog

		cryptoHelper, err = cryptohelper.NewCryptoHelper(mclient, []byte(cfg.E2EE.PickleKey), cfg.E2EE.DBPath)
		if err != nil {
			return nil, fmt.Errorf("create crypto helper: %w", err)
		}
		cryptoHelper.DBAccountID = "guiltyspark-bot"
	}

	batchIndexer.SendReceiptFn = func(roomID, eventID string) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := mclient.SendReceipt(ctx, id.RoomID(roomID), id.EventID(eventID), event.ReceiptTypeRead, nil); err != nil {
			log.Printf("Failed to send read receipt for %s in %s: %v", eventID, roomID, err)
		}
	}

	return &Bot{
		cfg:               cfg,
		client:            mclient,
		embedClient:       embedClient,
		bleveClient:       bleveClient,
		batchIndexer:      batchIndexer,
		imageProcessor:    imageProcessor,
		commandHandler:    commandHandler,
		searchEngine:      searchEngine,
		gracePeriod:       gracePeriod,
		joinedInviteRooms: make(map[string]bool),
		cryptoHelper:      cryptoHelper,
	}, nil
}

func (b *Bot) Start() error {
	// Parse server name from homeserver URL
	parsedServer := id.ParseServerName(b.cfg.Bot.Homeserver)
	if parsedServer == nil {
		return fmt.Errorf("invalid homeserver URL: %s", b.cfg.Bot.Homeserver)
	}

	// Set syncer (client and state store already created in New())
	b.client.Syncer = mautrix.NewDefaultSyncer()

	// Set up event handlers
	syncer := b.client.Syncer.(*mautrix.DefaultSyncer)
	syncer.OnEventType(event.EventMessage, func(ctx context.Context, ev *event.Event) {
		go b.handleEvent(ev)
	})
	// Auto-join when invited to a room (DM or otherwise)
	syncer.OnEventType(event.StateMember, func(ctx context.Context, ev *event.Event) {
		b.handleMembershipEvent(ev)
	})
	// Handle invite events from sync response
	syncer.OnSync(b.handleSyncResponse)

	// Initialize E2EE crypto helper — this sets up decryption for m.room.encrypted events
	// and registers its own handlers for ProcessSyncResponse, HandleMemberEvent, HandleEncrypted
	ctx := context.Background()
	initErr := b.cryptoHelper.Init(ctx)

	// When login_password is set, cryptohelper logs in via StoreCredentials
	// which sets client.AccessToken = resp.AccessToken on the client.
	// But we also check LoginAs.Token in case the flow used token login.
	if b.cfg.E2EE.LoginPassword != "" && b.client.AccessToken == "" {
		log.Println("WARNING: client.AccessToken is empty after Init, trying fallback...")
		if b.cryptoHelper.LoginAs != nil && b.cryptoHelper.LoginAs.Token != "" {
			b.client.AccessToken = b.cryptoHelper.LoginAs.Token
		}
	}

	// Fallback: use existing access token approach with ShareKeys workaround
	// (only when login_password is not set)
	if initErr != nil && b.cfg.E2EE.LoginPassword == "" {
		log.Println("Fallback E2EE: re-registering sync listeners and sharing keys...")
		syncer := b.client.Syncer.(mautrix.ExtensibleSyncer)
		syncer.OnSync(b.cryptoHelper.Machine().ProcessSyncResponse)
		syncer.OnEventType(event.StateMember, b.cryptoHelper.Machine().HandleMemberEvent)
		if _, ok := b.client.Syncer.(mautrix.DispatchableSyncer); ok {
			syncer.OnEventType(event.EventEncrypted, b.cryptoHelper.HandleEncrypted)
		}

		if strings.Contains(initErr.Error(), "not marked as shared") {
			log.Println("Existing non-E2EE device found on server, sharing fresh Olm keys...")
			if err := b.cryptoHelper.Machine().ShareKeys(ctx, -1); err != nil {
				if strings.Contains(err.Error(), "already exists") || strings.Contains(err.Error(), "M_UNKNOWN") {
					log.Println("One-time keys already on server, continuing without ShareKeys")
				} else {
					return fmt.Errorf("share keys: %w", err)
				}
			} else {
				log.Println("E2EE keys shared, continuing with fresh account")
			}
			initErr = nil
		}
	}
	if initErr != nil {
		return fmt.Errorf("init e2ee crypto: %w", initErr)
	}
	log.Printf("E2EE crypto initialized (device: %s, token: %s)", b.client.DeviceID, func() string {
		if b.client.AccessToken != "" {
			return b.client.AccessToken[:10] + "..."
		}
		return "<empty>"
	}())

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

	// Start history scan for configured rooms
	for key, roomCfg := range b.cfg.Rooms {
		if roomCfg.HistoryScanCutoff == "" {
			continue
		}
		resolvedID, err := b.resolveRoomKeyToID(key)
		if err != nil {
			log.Printf("WARN bot: failed to resolve room key %q: %v", key, err)
			continue
		}
		go b.ScanRoomHistory(string(resolvedID), roomCfg.ScanCutoffUnix)
	}

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
			if slices.Contains(rooms, ev.RoomID) {
					return true
				}
		}
		// Room not in m.direct — fall back to member count check
		// (e.g. newly created DMs, or unencrypted DMs not synced to m.direct)
	}

	// Check the room joined_members count
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

// stripCommandAlias removes a command alias prefix from the beginning of text.
// Handles both "!alias query" and "alias query" (when ! was already stripped).
func stripCommandAlias(text, command string) string {
	if text == "" || command == "" {
		return text
	}
	allAliases := commandAliases[command]
	candidates := append([]string{command}, allAliases...)
	for _, alias := range candidates {
		for _, prefix := range []string{alias + " ", "!" + alias + " "} {
			if strings.HasPrefix(text, prefix) {
				return strings.TrimSpace(strings.TrimPrefix(text, prefix))
			}
		}
	}
	return text
}

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
// Returns resolved room ID, resolved user ID, and cleaned HTML without the links
// and command alias prefix.
func (b *Bot) parseRoomAndUserFromHTML(html, command string) (string, string, string) {
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

	// Strip command alias prefix (e.g. "!с" or "!s") from cleaned HTML
	cleanedHTML = stripCommandAlias(cleanedHTML, command)

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

// resolveRoomKeyToID resolves a config key that may be a room alias or room ID to an actual room ID.
func (b *Bot) resolveRoomKeyToID(key string) (id.RoomID, error) {
	if strings.HasPrefix(key, "#") {
		alias := id.RoomAlias(key)
		roomID, err := b.resolveAliasToRoomID(alias)
		if err != nil {
			return "", fmt.Errorf("resolve alias %q: %w", key, err)
		}
		return roomID, nil
	}
	// Already a room ID or something else — treat as room ID directly
	return id.RoomID(key), nil
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
					searchRoomID, searchUser, formattedBody = b.parseRoomAndUserFromHTML(formattedBody, cmd)
					args = formattedBody
				} else {
					// Strip command alias from args before room resolution so the regex
					// doesn't match the alias (e.g. "s:room_name" instead of "#room_name:server").
					args = stripCommandAlias(args, cmd)
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
			previews := b.fetchPreviews(text)
			text = buildTextWithPreviews(text, previews)
		}

		msg := indexer.PendingMessage{
			EventID:   ev.ID.String(),
			RoomID:    ev.RoomID.String(),
			UserID:    ev.Sender.String(),
			Timestamp: ev.Timestamp,
			EventType: "m.room.message",
			Text:      text,
		}

		b.batchIndexer.OnTextMessageWithBuffering(msg)
		if b.batchIndexer.SendReceiptFn != nil {
			lastEid := b.batchIndexer.BufferedLastEventID(ev.RoomID.String())
			if lastEid != "" {
				b.batchIndexer.SendReceiptFn(ev.RoomID.String(), lastEid)
			}
		}
	} else if body.MsgType == event.MsgImage {
		// Index as image message
		img := indexer.PendingImage{
			EventID:   ev.ID.String(),
			RoomID:    ev.RoomID.String(),
			UserID:    ev.Sender.String(),
			Timestamp: ev.Timestamp,
			RawURL:    string(body.URL),
			FileName:  body.Body,
			MimeType:  body.Info.MimeType,
		}

		b.batchIndexer.OnImageMessage(img)
		if b.batchIndexer.SendReceiptFn != nil {
			b.batchIndexer.SendReceiptFn(ev.RoomID.String(), ev.ID.String())
		}
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

	// Join the room in a goroutine to avoid blocking the sync loop
	go func() {
		resp, err := b.client.JoinRoom(context.Background(), ev.RoomID.String(), nil)
		if err != nil {
			log.Printf("Failed to join room %s: %v", ev.RoomID, err)
			return
		}

		log.Printf("Bot joined room %s (new room ID: %s)", ev.RoomID, resp.RoomID)
	}()
}

// handleSyncResponse processes sync responses and auto-joins invited rooms,
// auto-leaves rooms where the bot is the only member.
func (b *Bot) handleSyncResponse(ctx context.Context, resp *mautrix.RespSync, since string) bool {
	// Auto-join invited rooms (skip already processed)
	for roomID := range resp.Rooms.Invite {
		b.inviteMu.Lock()
		if b.joinedInviteRooms[roomID.String()] {
			b.inviteMu.Unlock()
			continue
		}
		b.joinedInviteRooms[roomID.String()] = true
		b.inviteMu.Unlock()

		log.Printf("Bot has invite to room %s, joining...", roomID)

		// For remote rooms, provide the server via Via
		var req *mautrix.ReqJoinRoom
		if strings.Contains(roomID.String(), ":") {
			parts := strings.SplitN(roomID.String(), ":", 2)
			if len(parts) == 2 {
				req = &mautrix.ReqJoinRoom{
					Reason: "Auto-joining via bot sync handler",
					Via:    []string{parts[1]},
				}
			}
		}

		// Join in a goroutine to avoid blocking the sync loop
		go func(rID id.RoomID, rReq *mautrix.ReqJoinRoom) {
			joinResp, err := b.client.JoinRoom(context.Background(), rID.String(), rReq)
			if err != nil {
				log.Printf("Failed to join room %s: %v", rID, err)
				return
			}
			log.Printf("Joined room %s (new room ID: %s)", rID, joinResp.RoomID)
		}(roomID, req)
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
	if b.cryptoHelper != nil {
		b.cryptoHelper.Close()
	}
}

// fetchPreviews extracts URLs from text, skips preview hosts, and fetches previews
// via the Matrix client's GetURLPreview API.
func (b *Bot) fetchPreviews(text string) []*event.LinkPreview {
	urls := extractURLs(text, b.cfg.LinkPreview.MaxURLs)
	if len(urls) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), b.cfg.LinkPreview.Timeout)
	defer cancel()

	var wg sync.WaitGroup
	mu := sync.Mutex{}
	var previews []*event.LinkPreview

	for _, u := range urls {
		wg.Add(1)
		go func(rawURL string) {
			defer wg.Done()
			preview, err := b.fetchPreviewViaAPI(ctx, rawURL)
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
	return previews
}

// fetchPreviewViaAPI fetches a URL preview by making a direct HTTP request
// to the homeserver's /preview_url endpoint with proper double URL encoding.
//
// We can't use mautrix's GetURLPreview because BuildURLWithQuery → url.Values.Encode()
// → url.QueryEscape in Go 1.19+ does not percent-encode non-ASCII bytes — they pass
// through as raw UTF-8. The Matrix server (Synapse) requires ASCII-only query params
// and rejects non-ASCII URLs with 400 or 502 errors. Additionally, BuildURLWithQuery
// doesn't allow custom query parameter encoding, so any manual double-encoding of the
// URL value would get re-encoded (e.g. ":" → "%3A", "/" → "%2F"), producing a triple-
// encoded URL that also fails. Direct HTTP request bypasses these mautrix limitations.
func (b *Bot) fetchPreviewViaAPI(ctx context.Context, targetURL string) (*event.LinkPreview, error) {
	// Double-encode the URL: first encode non-ASCII bytes to %XX, then encode % as %25
	// to match what Element sends to the homeserver.
	encodedURL := doubleEncodeURL(targetURL)

	// Build the homeserver API URL directly with proper query parameter encoding
	homeserverURL := b.client.HomeserverURL.String()
	fullURL := homeserverURL + "/_matrix/client/v1/media/preview_url?url=" + encodedURL

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+b.client.AccessToken)

	resp, err := b.client.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var apiErr struct {
			ErrCode string `json:"errcode"`
			Error   string `json:"error"`
		}
		if decodeErr := json.NewDecoder(resp.Body).Decode(&apiErr); decodeErr == nil && apiErr.ErrCode != "" {
			return nil, fmt.Errorf("M_%s (HTTP %d): %s", apiErr.ErrCode, resp.StatusCode, apiErr.Error)
		}
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	var output event.LinkPreview
	if err := json.NewDecoder(resp.Body).Decode(&output); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &output, nil
}

// doubleEncodeURL produces the same encoding that Element uses for the Matrix
// homeserver /preview_url endpoint: non-ASCII bytes are double-percent-encoded.
func doubleEncodeURL(raw string) string {
	// Step 1: encode non-ASCII bytes to %XX
	step1 := ""
	for i := 0; i < len(raw); i++ {
		if raw[i] < 128 {
			step1 += string(raw[i])
		} else {
			step1 += fmt.Sprintf("%%%02X", raw[i])
		}
	}

	// Step 2: encode % → %25 (preserving already-encoded %XX sequences)
	result := ""
	for i := 0; i < len(step1); i++ {
		if step1[i] == '%' {
			result += "%25"
		} else {
			result += string(step1[i])
		}
	}
	return result
}

// ScanRoomHistory scans a room's message history backwards from the present
// and indexes new messages until a cutoff date, room start, or stale pages threshold is reached.
func (b *Bot) ScanRoomHistory(roomID string, cutoffUnix int64) {
	var textsIndexed int
	var imagesDeferred int
	var pagesScanned int
	stalePages := 0
	from := ""

	log.Printf("INFO bot: starting history scan for room %s (cutoff: %d)", roomID, cutoffUnix)

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		resp, err := b.client.Messages(ctx, id.RoomID(roomID), from, "", mautrix.Direction('b'), nil, 100)
		cancel()

		if err != nil {
			log.Printf("WARN bot: failed to fetch messages for room %s: %v", roomID, err)
			break
		}

		if len(resp.Chunk) == 0 {
			break
		}

		events := make([]*event.Event, len(resp.Chunk))
		copy(events, resp.Chunk)
		for i, j := 0, len(events)-1; i < j; i, j = i+1, j-1 {
			events[i], events[j] = events[j], events[i]
		}

		newestTs := events[len(events)-1].Timestamp
		if cutoffUnix > 0 && newestTs/1000 <= cutoffUnix {
			break
		}

		hasNewEvents := false
		hasProcessableEvents := false
		for _, ev := range events {
			eventTime := time.UnixMilli(ev.Timestamp)
			if cutoffUnix > 0 && eventTime.Unix() <= cutoffUnix {
				continue
			}
			if ev.Sender.String() == b.cfg.Bot.UserID {
				textsIndexed += b.batchIndexer.FlushRoom(ev.RoomID.String())
				continue
			}
			if ev.Type.Type != event.EventMessage.Type {
				continue
			}
			if err := ev.Content.ParseRaw(ev.Type); err != nil {
				continue
			}
			body, ok := ev.Content.Parsed.(*event.MessageEventContent)
			if !ok {
				continue
			}
			if body.MsgType == event.MsgText && len(body.Body) > 0 && body.Body[0] == '!' {
				textsIndexed += b.batchIndexer.FlushRoom(ev.RoomID.String())
				continue
			}

			hasProcessableEvents = true

			switch body.MsgType {
			case event.MsgText:
				text := body.Body
				msg := indexer.PendingMessage{
					EventID:   ev.ID.String(),
					RoomID:    ev.RoomID.String(),
					UserID:    ev.Sender.String(),
					Timestamp: ev.Timestamp,
					EventType: "m.room.message",
					Text:      text,
				}
				indexed := b.batchIndexer.OnTextMessageWithBuffering(msg)
				textsIndexed += indexed
				if indexed > 0 {
					hasNewEvents = true
				}
			case event.MsgImage:
				img := indexer.PendingImage{
					EventID:   ev.ID.String(),
					RoomID:    ev.RoomID.String(),
					UserID:    ev.Sender.String(),
					Timestamp: ev.Timestamp,
					RawURL:    string(body.URL),
					FileName:  body.Body,
					MimeType:  body.Info.MimeType,
				}
				if added := b.batchIndexer.QueueImage(img); added {
					imagesDeferred++
					hasNewEvents = true
				}
			}
		}

		pagesScanned++

		if !hasProcessableEvents {
			from = resp.End
			if from == "" && len(events) > 0 {
				break
			}
			continue
		}

		if hasNewEvents {
			stalePages = 0
		} else {
			stalePages++
			if stalePages >= b.cfg.HistoryScan.StalePagesThreshold {
				log.Printf("INFO bot: reached stale pages threshold (%d) for room %s", stalePages, roomID)
				break
			}
		}

		from = resp.End
		if from == "" && len(events) > 0 {
			break
		}
	}

	flushed := b.batchIndexer.FlushBufferedMessages()
	textsIndexed += flushed

	log.Printf("INFO bot: scanned room %s: %d texts indexed, %d images deferred, %d pages scanned", roomID, textsIndexed, imagesDeferred, pagesScanned)
}
