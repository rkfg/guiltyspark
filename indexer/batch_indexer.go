package indexer

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type PendingMessage struct {
	EventID   string
	RoomID    string
	UserID    string
	Timestamp int64
	EventType string
	Text      string
}

type BatchIndexer struct {
	pendingMessages []PendingMessage
	pendingImages   []PendingImage
	// deferredImages are images whose LLM processing (VLM + embedding) is deferred
	// until the daily scheduled time (configurable via delayed_embed_hour/minute).
	deferredImages []PendingImage
	// deferredTextEmbed are text messages whose embedding is deferred
	// until the daily scheduled time.
	deferredTextEmbed []PendingMessage
	batchTimeout    time.Duration
	maxBatchDelay   time.Duration
	lastMessageTime time.Time
	firstPendingTime  time.Time
	mu              sync.Mutex
	stopCh          chan struct{}
	doneCh          chan struct{}

	// Callbacks
	IndexTextFn   func(doc IndexedDocument) error
	IsIndexedFn   func(eventID string) (bool, error)
	ImageProcFn   func(img PendingImage) error // Process image (download, convert, describe)

	// Deferred processing
	ProcessDeferredFn       func(images []PendingImage) error
	ProcessDeferredTextFn   func(texts []PendingMessage) error

	// Channels for non-blocking ingestion
	msgCh    chan PendingMessage
	imageCh  chan PendingImage

	// Deferred processing scheduler
	embedHour   int
	embedMinute int
}

type PendingImage struct {
	EventID   string
	RoomID    string
	UserID    string
	Timestamp int64
	RawURL    string
	FileName  string
	MimeType  string
}

func NewBatchIndexer(batchTimeout, maxBatchDelay time.Duration, embedHour, embedMinute int) *BatchIndexer {
	b := &BatchIndexer{
		batchTimeout:   batchTimeout,
		maxBatchDelay:  maxBatchDelay,
		stopCh:         make(chan struct{}),
		doneCh:         make(chan struct{}),
		msgCh:          make(chan PendingMessage, 1000),
		imageCh:        make(chan PendingImage, 1000),
		embedHour:      embedHour,
		embedMinute:    embedMinute,
	}
	// Start ingestion goroutine
	go b.ingestLoop()
	// Start deferred processing scheduler
	go b.deferredProcessingLoop()
	return b
}

// OnTextMessage is non-blocking — enqueues message via channel
func (b *BatchIndexer) OnTextMessage(msg PendingMessage) {
	select {
	case b.msgCh <- msg:
	default:
	}
}

// OnImageMessage is non-blocking — enqueues message via channel
func (b *BatchIndexer) OnImageMessage(img PendingImage) {
	select {
	case b.imageCh <- img:
	default:
	}
}

func (b *BatchIndexer) ingestLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case msg := <-b.msgCh:
			b.mu.Lock()
			b.pendingMessages = append(b.pendingMessages, msg)
			b.lastMessageTime = time.Now()
			if len(b.pendingMessages) == 1 && len(b.pendingImages) == 0 {
				b.firstPendingTime = time.Now()
			}
			b.mu.Unlock()

		case img := <-b.imageCh:
			b.mu.Lock()
			b.pendingImages = append(b.pendingImages, img)
			b.lastMessageTime = time.Now()
			if len(b.pendingMessages) == 0 && len(b.pendingImages) == 1 {
				b.firstPendingTime = time.Now()
			}
			b.mu.Unlock()

		case <-ticker.C:
			b.mu.Lock()
			hasPending := len(b.pendingMessages) > 0 || len(b.pendingImages) > 0
			if hasPending && time.Since(b.lastMessageTime) > b.batchTimeout {
				b.savePending()
				b.flush()
				b.removePending()
			}
			b.mu.Unlock()

		case <-b.stopCh:
			b.mu.Lock()
			b.savePending()
			b.flush()
			b.removePending()
			b.mu.Unlock()
			close(b.doneCh)
			return
		}
	}
}

// deferredProcessingLoop waits until the configured daily time and processes
// all deferred images (VLM description + embedding) in one batch.
func (b *BatchIndexer) deferredProcessingLoop() {
	for {
		now := time.Now()
		next := time.Date(now.Year(), now.Month(), now.Day(),
			b.embedHour, b.embedMinute, 0, 0, now.Location())
		if next.Before(now) || next.Equal(now) {
			next = next.Add(24 * time.Hour)
		}

		select {
		case <-time.After(time.Until(next)):
			b.mu.Lock()
			if len(b.deferredImages) > 0 {
				log.Printf("INFO batch_indexer: processing %d deferred images", len(b.deferredImages))
				if b.ProcessDeferredFn != nil {
					deferred := make([]PendingImage, len(b.deferredImages))
					copy(deferred, b.deferredImages)
					b.deferredImages = b.deferredImages[:0]
					b.mu.Unlock()
					if err := b.ProcessDeferredFn(deferred); err != nil {
						log.Printf("ERROR batch_indexer: deferred processing error: %v", err)
					}
				} else {
					b.mu.Unlock()
				}
			} else {
				b.mu.Unlock()
			}
			if len(b.deferredTextEmbed) > 0 {
				log.Printf("INFO batch_indexer: processing %d deferred text embeddings", len(b.deferredTextEmbed))
				if b.ProcessDeferredTextFn != nil {
					deferred := make([]PendingMessage, len(b.deferredTextEmbed))
					copy(deferred, b.deferredTextEmbed)
					b.deferredTextEmbed = b.deferredTextEmbed[:0]
					b.mu.Unlock()
					if err := b.ProcessDeferredTextFn(deferred); err != nil {
						log.Printf("ERROR batch_indexer: deferred text embedding error: %v", err)
					}
				} else {
					b.mu.Unlock()
				}
			} else {
				b.mu.Unlock()
			}
		case <-b.stopCh:
			return
		}
	}
}

func (b *BatchIndexer) Stop() {
	close(b.stopCh)
	<-b.doneCh
}

func (b *BatchIndexer) flush() {
	if len(b.pendingMessages) == 0 && len(b.pendingImages) == 0 {
		return
	}

	// Pre-allocate deferred slices to avoid repeated reallocation.
	deferredText := make([]PendingMessage, 0, len(b.pendingMessages))
	deferredImages := make([]PendingImage, 0, len(b.pendingImages))

	for _, msg := range b.pendingMessages {
		b.indexTextMessage(msg)
		deferredText = append(deferredText, msg)
	}

	for _, img := range b.pendingImages {
		deferredImages = append(deferredImages, img)
	}

	b.deferredTextEmbed = append(b.deferredTextEmbed, deferredText...)
	b.deferredImages = append(b.deferredImages, deferredImages...)

	// Reuse the underlying arrays instead of allocating new ones.
	b.pendingMessages = b.pendingMessages[:0]
	b.pendingImages = b.pendingImages[:0]
	b.firstPendingTime = time.Time{}
}

func (b *BatchIndexer) indexTextMessage(msg PendingMessage) {
	// Check dedup via Bleve
	if b.IsIndexedFn != nil {
		indexed, err := b.IsIndexedFn(msg.EventID)
		if err != nil {
			log.Printf("ERROR batch_indexer: IsIndexedFn error for event %s: %v", msg.EventID, err)
			return
		}
		if indexed {
			return
		}
	}

	// Immediate Bleve indexing (no embedding — done later)
	doc := IndexedDocument{
		ID:        fmt.Sprintf("%s:%s", msg.RoomID, msg.EventID),
		EventID:   msg.EventID,
		RoomID:    msg.RoomID,
		UserID:    msg.UserID,
		Timestamp: msg.Timestamp,
		EventType: msg.EventType,
		Text:      msg.Text,
	}

	if err := b.IndexTextFn(doc); err != nil {
		log.Printf("ERROR batch_indexer: IndexTextFn error for event %s: %v", msg.EventID, err)
		return
	}
}

func (b *BatchIndexer) savePending() {
	dir := filepath.Join(".", "bot-data", "pending")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return
	}

	data, _ := json.Marshal(map[string]interface{}{
		"text":   b.pendingMessages,
		"images": b.pendingImages,
	})
	os.WriteFile(filepath.Join(dir, "pending.json"), data, 0644)
}

func (b *BatchIndexer) removePending() {
	os.Remove(filepath.Join(".", "bot-data", "pending", "pending.json"))
}
