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
	batchTimeout    time.Duration
	maxBatchDelay   time.Duration
	lastMessageTime time.Time
	firstPendingTime  time.Time
	mu              sync.Mutex
	stopCh          chan struct{}
	doneCh          chan struct{}

	// Callbacks
	IndexTextFn   func(doc IndexedDocument) error
	IndexImageFn  func(doc IndexedDocument, vector []float32, imageDesc string) error
	EmbedTextFn   func(text string) ([]float32, error)
	IsIndexedFn   func(eventID string) (bool, error)
	ImageProcFn   func(img PendingImage) error // Process image (download, convert, describe)

	// Channels for non-blocking ingestion
	msgCh    chan PendingMessage
	imageCh  chan PendingImage
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

func NewBatchIndexer(batchTimeout, maxBatchDelay time.Duration) *BatchIndexer {
	b := &BatchIndexer{
		batchTimeout:   batchTimeout,
		maxBatchDelay:  maxBatchDelay,
		stopCh:         make(chan struct{}),
		doneCh:         make(chan struct{}),
		msgCh:          make(chan PendingMessage, 1000), // buffered for non-blocking
		imageCh:        make(chan PendingImage, 1000),    // buffered for non-blocking
	}
	// Start ingestion goroutine
	go b.ingestLoop()
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

func (b *BatchIndexer) Stop() {
	close(b.stopCh)
	<-b.doneCh
}

func (b *BatchIndexer) flush() {
	if len(b.pendingMessages) == 0 && len(b.pendingImages) == 0 {
		return
	}

	// Process text messages
	for _, msg := range b.pendingMessages {
		b.processTextMessage(msg)
	}

	// Process images
	for _, img := range b.pendingImages {
		b.processImageMessage(img)
	}

	b.pendingMessages = b.pendingMessages[:0]
	b.pendingImages = b.pendingImages[:0]
	b.firstPendingTime = time.Time{}
}

func (b *BatchIndexer) processTextMessage(msg PendingMessage) {
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

	// Embed text
	vector, err := b.EmbedTextFn(msg.Text)
	if err != nil {
		log.Printf("ERROR batch_indexer: EmbedTextFn error for event %s: %v", msg.EventID, err)
		return
	}

	doc := IndexedDocument{
		ID:        fmt.Sprintf("%s:%s", msg.RoomID, msg.EventID),
		EventID:   msg.EventID,
		RoomID:    msg.RoomID,
		UserID:    msg.UserID,
		Timestamp: msg.Timestamp,
		EventType: msg.EventType,
		Text:      msg.Text,
		Vector:    vector,
	}

	if err := b.IndexTextFn(doc); err != nil {
		log.Printf("ERROR batch_indexer: IndexTextFn error for event %s: %v", msg.EventID, err)
		return
	}
}

func (b *BatchIndexer) processImageMessage(img PendingImage) {
	// Check dedup via Bleve
	if b.IsIndexedFn != nil {
		indexed, err := b.IsIndexedFn(img.EventID)
		if err != nil {
			log.Printf("ERROR batch_indexer: IsIndexedFn error for image event %s: %v", img.EventID, err)
			return
		}
		if indexed {
			return
		}
	}

	if b.ImageProcFn != nil {
		if err := b.ImageProcFn(img); err != nil {
			log.Printf("ERROR batch_indexer: ImageProcFn error for event %s: %v", img.EventID, err)
			return
		}
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
