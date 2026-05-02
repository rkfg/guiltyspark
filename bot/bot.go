package bot

import (
	"context"
	"fmt"
	"log"
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
	batchIndexer := indexer.NewBatchIndexer(cfg.Indexing.BatchTimeout, cfg.Indexing.MaxBatchDelay, cfg.Indexing.DelayedEmbedHour, cfg.Indexing.DelayedEmbedMinute)

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
	batchIndexer.ImageProcFn = func(img indexer.PendingImage) error {
		result, err := imageProcessor.ProcessImage(img.RawURL, img.RoomID, img.UserID, img.EventID, img.Timestamp, img.RawURL, img.FileName, img.MimeType)
		if err != nil {
			return err
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

		// Index in Bleve for text search (struct preserves []float32 type)
		if err := bleveClient.IndexDocumentStruct(doc); err != nil {
			return err
		}
		return nil
	}
	batchIndexer.ProcessDeferredFn = func(images []indexer.PendingImage) error {
		for _, img := range images {
			result, err := imageProcessor.ProcessImage(img.RawURL, img.RoomID, img.UserID, img.EventID, img.Timestamp, img.RawURL, img.FileName, img.MimeType)
			if err != nil {
				log.Printf("Failed to process deferred image %s: %v", img.EventID, err)
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
				continue
			}
		}
		return nil
	}
	batchIndexer.ProcessDeferredTextFn = func(texts []indexer.PendingMessage) error {
		for _, msg := range texts {
			vector, err := embedClient.CreateEmbedding(msg.Text)
			if err != nil {
				log.Printf("Failed to embed deferred text %s: %v", msg.EventID, err)
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
				continue
			}
		}
		return nil
	}

	return &Bot{
		cfg:            cfg,
		embedClient:    embedClient,
		bleveClient:    bleveClient,
		batchIndexer:   batchIndexer,
		imageProcessor: imageProcessor,
		commandHandler: commandHandler,
		searchEngine:   searchEngine,
		gracePeriod:    gracePeriod,
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
	b.client.StateStore = &mautrix.MemoryStateStore{}
	b.client.Syncer = mautrix.NewDefaultSyncer()

	// Set up event handler
	syncer := b.client.Syncer.(*mautrix.DefaultSyncer)
	syncer.OnEventType(event.EventMessage, func(ctx context.Context, ev *event.Event) {
		b.handleEvent(ev)
	})

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
				switch cmd {
				case "search":
					textResult, htmlResult, err := b.commandHandler.HandleSearch(args, ev.RoomID.String())
					if err != nil {
						textResult = fmt.Sprintf("Error: %v", err)
						htmlResult = textResult
					}
					b.sendReplyHTML(ev.RoomID, ev.ID, textResult, htmlResult)
				case "search-semantic":
					textResult, htmlResult, err := b.commandHandler.HandleSemanticSearch(args, ev.RoomID.String())
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
		msg := indexer.PendingMessage{
			EventID:   ev.ID.String(),
			RoomID:    ev.RoomID.String(),
			UserID:    ev.Sender.String(),
			Timestamp: ev.Timestamp,
			EventType: "m.room.message",
			Text:      body.Body,
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

func (b *Bot) Stop() {
	b.batchIndexer.Stop()
	b.bleveClient.Close()
	b.client.StopSync()
}
