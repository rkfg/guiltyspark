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

// messageBuffer accumulates consecutive messages from the same user in the same room.
type messageBuffer struct {
	firstEventID string
	allEventIDs  []string
	text         string
	RoomID       string
	UserID       string
	Timestamp    int64
}

type BatchIndexer struct {
	msgBuffer map[string]*messageBuffer
	// deferredImages are images whose LLM processing (VLM + embedding) is deferred
	deferredImages []PendingImage
	// deferredTextEmbed are text messages whose embedding is deferred
	deferredTextEmbed []PendingMessage
	mu                sync.Mutex
	saveMu            sync.Mutex
	stopCh            chan struct{}
	wg                sync.WaitGroup

	// Callbacks
	IndexTextFn  func(doc IndexedDocument) error
	IsIndexedFn  func(eventID string) (bool, error)
	AddEventIDFn func(eventID string) error

	// Deferred processing
	ProcessImageDescFn func(images []PendingImage) ([]PendingImage, error)
	ProcessImageEmbedFn func(images []PendingImage) ([]PendingImage, error)
	ProcessDeferredTextFn func(texts []PendingMessage) ([]PendingMessage, error)

	// Channels for non-blocking ingestion
	imageCh chan PendingImage

	// Callbacks
	SendReceiptFn func(roomID, eventID string)

	// Deferred processing scheduler
	embedHour   int
	embedMinute int
	persistDir  string
}

type PendingImage struct {
	EventID     string
	RoomID      string
	UserID      string
	Timestamp   int64
	RawURL      string
	FileName    string
	MimeType    string
	Description string
}

const persistFile = "deferred.json"

func NewBatchIndexer(embedHour, embedMinute int, persistDir string) *BatchIndexer {
	b := &BatchIndexer{
		msgBuffer: make(map[string]*messageBuffer),
		stopCh:    make(chan struct{}),
		imageCh:   make(chan PendingImage, 1000),
		embedHour: embedHour,
		embedMinute: embedMinute,
		persistDir:  persistDir,
	}

	b.loadDeferred()

	b.wg.Add(1)
	go b.ingestLoop()
	b.wg.Add(1)
	go b.deferredProcessingLoop()
	b.wg.Add(1)
	go b.periodicSaveLoop()
	return b
}

type persistData struct {
	DeferredImages    []PendingImage   `json:"deferred_images"`
	DeferredTextEmbed []PendingMessage `json:"deferred_text"`
}

func (b *BatchIndexer) saveDeferred() {
	b.saveMu.Lock()
	defer b.saveMu.Unlock()
	b.mu.Lock()
	data := persistData{
		DeferredImages:    make([]PendingImage, len(b.deferredImages)),
		DeferredTextEmbed: make([]PendingMessage, len(b.deferredTextEmbed)),
	}
	copy(data.DeferredImages, b.deferredImages)
	copy(data.DeferredTextEmbed, b.deferredTextEmbed)
	b.mu.Unlock()

	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Printf("ERROR batch_indexer: failed to marshal deferred data: %v", err)
		return
	}

	file := filepath.Join(b.persistDir, persistFile)
	tmpFile := file + ".tmp"
	if err := os.WriteFile(tmpFile, jsonData, 0644); err != nil {
		log.Printf("ERROR batch_indexer: failed to save deferred data: %v", err)
		return
	}
	if err := os.Rename(tmpFile, file); err != nil {
		log.Printf("ERROR batch_indexer: failed to rename deferred data: %v", err)
	}
}

func (b *BatchIndexer) loadDeferred() {
	file := filepath.Join(b.persistDir, persistFile)
	jsonData, err := os.ReadFile(file)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		log.Printf("ERROR batch_indexer: failed to load deferred data: %v", err)
		return
	}

	var data persistData
	if err := json.Unmarshal(jsonData, &data); err != nil {
		log.Printf("ERROR batch_indexer: failed to unmarshal deferred data: %v", err)
		return
	}

	b.mu.Lock()
	b.deferredImages = append(b.deferredImages, data.DeferredImages...)
	b.deferredTextEmbed = append(b.deferredTextEmbed, data.DeferredTextEmbed...)
	b.mu.Unlock()

	log.Printf("INFO batch_indexer: loaded %d deferred images, %d deferred text embeddings", len(data.DeferredImages), len(data.DeferredTextEmbed))
}

// OnTextMessageWithBuffering accumulates consecutive messages from the same user in the same room.
// Buffer is keyed by roomID. When a different user writes in the same room, the previous buffer is flushed.
// Same user writing again: appends to existing buffer, re-indexes.
// Returns the number of documents actually indexed during this call.
func (b *BatchIndexer) OnTextMessageWithBuffering(msg PendingMessage) int {
	b.mu.Lock()

	var count int

	if existing, exists := b.msgBuffer[msg.RoomID]; exists {
		if existing.UserID == msg.UserID {
			existing.text = existing.text + "\n" + msg.Text
			existing.allEventIDs = append(existing.allEventIDs, msg.EventID)
			existing.Timestamp = msg.Timestamp
			b.reindexBufferLocked(existing)
			b.mu.Unlock()
			return 0
		}
		count = b.flushBufferLocked(existing)
		delete(b.msgBuffer, msg.RoomID)
	}

	b.msgBuffer[msg.RoomID] = &messageBuffer{
		firstEventID: msg.EventID,
		allEventIDs:  []string{msg.EventID},
		text:         msg.Text,
		RoomID:       msg.RoomID,
		UserID:       msg.UserID,
		Timestamp:    msg.Timestamp,
	}

	b.mu.Unlock()

	return count
}

// flushBufferLocked indexes the accumulated buffer and queues it for deferred embedding.
// Must be called with b.mu held. Does NOT call saveDeferred — caller must do that after unlocking.
// Returns 1 if the document was indexed, 0 if skipped (already indexed).
func (b *BatchIndexer) flushBufferLocked(buf *messageBuffer) int {
	msg := PendingMessage{
		EventID:   buf.firstEventID,
		RoomID:    buf.RoomID,
		UserID:    buf.UserID,
		Timestamp: buf.Timestamp,
		EventType: "m.room.message",
		Text:      buf.text,
	}
	if b.indexTextMessage(msg) {
		// Mark all event IDs from this buffer as processed
		if b.AddEventIDFn != nil {
			for _, eid := range buf.allEventIDs {
				if err := b.AddEventIDFn(eid); err != nil {
					log.Printf("Failed to mark event ID %s as processed: %v", eid, err)
				}
			}
		}
		b.deferredTextEmbed = append(b.deferredTextEmbed, msg)
		return 1
	}
	return 0
}

// reindexBufferLocked updates the buffer text after a new message from the same user.
// Actual indexing happens only at flush time (flushBufferLocked/FlushRoom/FlushBufferedMessages).
func (b *BatchIndexer) reindexBufferLocked(buf *messageBuffer) {
	// Text already appended by caller in OnTextMessageWithBuffering
}

// OnImageMessage is non-blocking — enqueues image via channel (with dedup).
func (b *BatchIndexer) OnImageMessage(img PendingImage) {
	select {
	case b.imageCh <- img:
	default:
	}
}

// QueueImage synchronously enqueues an image for deferred processing.
// Returns true if the image was added (false if already indexed).
// Used during history scan where sync dedup check is needed.
func (b *BatchIndexer) QueueImage(img PendingImage) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.isIndexed(img.EventID) {
		return false
	}
	b.deferredImages = append(b.deferredImages, img)
	if b.AddEventIDFn != nil {
		_ = b.AddEventIDFn(img.EventID)
	}
	return true
}

func (b *BatchIndexer) ingestLoop() {
	defer b.wg.Done()
	for {
		select {
		case img := <-b.imageCh:
			b.mu.Lock()
			if !b.isIndexed(img.EventID) {
				b.deferredImages = append(b.deferredImages, img)
				if b.AddEventIDFn != nil {
					_ = b.AddEventIDFn(img.EventID)
				}
			}
			b.mu.Unlock()

		case <-b.stopCh:
			return
		}
	}
}

func (b *BatchIndexer) isIndexed(eventID string) bool {
	if b.IsIndexedFn != nil {
		indexed, _ := b.IsIndexedFn(eventID)
		return indexed
	}
	return false
}

func (b *BatchIndexer) periodicSaveLoop() {
	defer b.wg.Done()
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			b.saveDeferred()
		case <-b.stopCh:
			return
		}
	}
}

// deferredProcessingLoop waits until the configured daily time and processes
// all deferred images in two phases: descriptions first, then embeddings.
func (b *BatchIndexer) deferredProcessingLoop() {
	defer b.wg.Done()
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
			deferredImages := make([]PendingImage, len(b.deferredImages))
			copy(deferredImages, b.deferredImages)
			deferredText := make([]PendingMessage, len(b.deferredTextEmbed))
			copy(deferredText, b.deferredTextEmbed)
			b.mu.Unlock()

			var failedImages []PendingImage
			var failedText []PendingMessage

			if len(deferredImages) > 0 {
				log.Printf("INFO batch_indexer: phase 1 - describing %d deferred images", len(deferredImages))
				if b.ProcessImageDescFn != nil {
					var err error
					deferredImages, err = b.ProcessImageDescFn(deferredImages)
					if err != nil {
						log.Printf("ERROR batch_indexer: image description error: %v", err)
					}
				}

				log.Printf("INFO batch_indexer: phase 2 - embedding %d image descriptions", len(deferredImages))
				if b.ProcessImageEmbedFn != nil {
					var err error
					failedImages, err = b.ProcessImageEmbedFn(deferredImages)
					if err != nil {
						log.Printf("ERROR batch_indexer: deferred images embedding error: %v", err)
					}
				}
			}

			if len(deferredText) > 0 {
				log.Printf("INFO batch_indexer: processing %d deferred text embeddings", len(deferredText))
				if b.ProcessDeferredTextFn != nil {
					var err error
					failedText, err = b.ProcessDeferredTextFn(deferredText)
					if err != nil {
						log.Printf("ERROR batch_indexer: deferred text processing error: %v", err)
					}
				}
			}

			b.mu.Lock()
			successImageIDs := make(map[string]bool)
			for _, img := range deferredImages {
				successImageIDs[img.EventID] = true
			}
			for _, img := range failedImages {
				delete(successImageIDs, img.EventID)
			}
			for i := 0; i < len(b.deferredImages); {
				if successImageIDs[b.deferredImages[i].EventID] {
					b.deferredImages = append(b.deferredImages[:i], b.deferredImages[i+1:]...)
				} else {
					i++
				}
			}

			successTextIDs := make(map[string]bool)
			for _, msg := range deferredText {
				successTextIDs[msg.EventID] = true
			}
			for _, msg := range failedText {
				delete(successTextIDs, msg.EventID)
			}
			for i := 0; i < len(b.deferredTextEmbed); {
				if successTextIDs[b.deferredTextEmbed[i].EventID] {
					b.deferredTextEmbed = append(b.deferredTextEmbed[:i], b.deferredTextEmbed[i+1:]...)
				} else {
					i++
				}
			}
			b.mu.Unlock()
			b.saveDeferred()
		case <-b.stopCh:
			return
		}
	}
}

func (b *BatchIndexer) Stop() {
	close(b.stopCh)
	b.wg.Wait()
	b.saveDeferred()
}

// FlushRoom flushes the buffer for a specific room.
// Returns the number of documents actually indexed (not skipped).
func (b *BatchIndexer) FlushRoom(roomID string) int {
	b.mu.Lock()
	var count int
	if buf, exists := b.msgBuffer[roomID]; exists {
		count = b.flushBufferLocked(buf)
		delete(b.msgBuffer, roomID)
	}
	b.mu.Unlock()
	return count
}

// FlushBufferedMessages flushes all pending message buffers by iterating
// the msgBuffer map and calling flushBufferLocked for each entry.
// Returns the count of documents that were actually indexed (not skipped).
func (b *BatchIndexer) FlushBufferedMessages() int {
	b.mu.Lock()
	keys := make([]string, 0, len(b.msgBuffer))
	for k := range b.msgBuffer {
		keys = append(keys, k)
	}
	b.mu.Unlock()

	flushed := 0
	for _, key := range keys {
		b.mu.Lock()
		buf, exists := b.msgBuffer[key]
		if exists {
			flushed += b.flushBufferLocked(buf)
			delete(b.msgBuffer, key)
		}
		b.mu.Unlock()
	}
	return flushed
}

func (b *BatchIndexer) indexTextMessage(msg PendingMessage) bool {
	if b.IsIndexedFn != nil {
		indexed, err := b.IsIndexedFn(msg.EventID)
		if err != nil {
			log.Printf("ERROR batch_indexer: IsIndexedFn error for event %s: %v", msg.EventID, err)
			return false
		}
		if indexed {
			log.Printf("INFO batch_indexer: skipping already indexed event %s", msg.EventID)
			return false
		}
	}

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
		return false
	}
	log.Printf("INFO batch_indexer: indexed text event %s", msg.EventID)
	return true
}
