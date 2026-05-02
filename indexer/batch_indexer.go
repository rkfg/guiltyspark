package indexer

import (
	"fmt"
	"log"
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
	pendingImages []PendingImage
	// deferredImages are images whose LLM processing (VLM + embedding) is deferred
	// until the daily scheduled time (configurable via delayed_embed_hour/minute).
	deferredImages []PendingImage
	// deferredTextEmbed are text messages whose embedding is deferred
	// until the daily scheduled time.
	deferredTextEmbed []PendingMessage
	lastMessageTime   time.Time
	mu                sync.Mutex
	stopCh            chan struct{}
	doneCh            chan struct{}

	// Callbacks
	IndexTextFn   func(doc IndexedDocument) error
	IsIndexedFn   func(eventID string) (bool, error)

	// Deferred processing
	ProcessDeferredFn       func(images []PendingImage) error
	ProcessDeferredTextFn   func(texts []PendingMessage) error

	// Channels for non-blocking ingestion
	imageCh chan PendingImage

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

func NewBatchIndexer(embedHour, embedMinute int) *BatchIndexer {
	b := &BatchIndexer{
		stopCh:      make(chan struct{}),
		doneCh:      make(chan struct{}),
		imageCh:     make(chan PendingImage, 1000),
		embedHour:   embedHour,
		embedMinute: embedMinute,
	}
	// Start ingestion goroutine
	go b.ingestLoop()
	// Start deferred processing scheduler
	go b.deferredProcessingLoop()
	return b
}

// OnTextMessage indexes text immediately (no batching).
func (b *BatchIndexer) OnTextMessage(msg PendingMessage) {
	b.indexTextMessage(msg)

	// Queue for deferred embedding
	b.mu.Lock()
	b.deferredTextEmbed = append(b.deferredTextEmbed, msg)
	b.mu.Unlock()
}

// OnImageMessage is non-blocking — enqueues image via channel
func (b *BatchIndexer) OnImageMessage(img PendingImage) {
	select {
	case b.imageCh <- img:
	default:
	}
}

func (b *BatchIndexer) ingestLoop() {
	for {
		select {
		case img := <-b.imageCh:
			b.mu.Lock()
			b.pendingImages = append(b.pendingImages, img)
			b.lastMessageTime = time.Now()
			b.mu.Unlock()

		case <-b.stopCh:
			b.mu.Lock()
			b.flushImages()
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

func (b *BatchIndexer) flushImages() {
	if len(b.pendingImages) == 0 {
		return
	}

	deferredImages := make([]PendingImage, len(b.pendingImages))
	copy(deferredImages, b.pendingImages)
	b.pendingImages = b.pendingImages[:0]

	b.deferredImages = append(b.deferredImages, deferredImages...)
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
	}
}
