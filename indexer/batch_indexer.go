package indexer

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
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
	firstEventID string // event ID of the first message in the sequence
	text         string // accumulated text joined by newlines
	RoomID       string
	UserID       string
	Timestamp    int64
}

type BatchIndexer struct {
	// msgBuffer stores accumulated messages by key "roomID|userID".
	// When a new message arrives from the same key, it replaces the buffer.
	// When a message arrives from a different key, the old buffer is flushed first.
	msgBuffer map[string]*messageBuffer
	// deferredImages are images whose LLM processing (VLM + embedding) is deferred
	// until the daily scheduled time (configurable via delayed_embed_hour/minute).
	deferredImages []PendingImage
	// deferredTextEmbed are text messages whose embedding is deferred
	// until the daily scheduled time.
	deferredTextEmbed []PendingMessage
	mu                sync.Mutex
	saveMu            sync.Mutex
	stopCh            chan struct{}
	wg                sync.WaitGroup

	// Callbacks
	IndexTextFn func(doc IndexedDocument) error
	IsIndexedFn func(eventID string) (bool, error)

	// Deferred processing
	// ProcessDeferredFn processes deferred images and returns list of failed items to retry
	ProcessDeferredFn     func(images []PendingImage) ([]PendingImage, error)
	ProcessDeferredTextFn func(texts []PendingMessage) ([]PendingMessage, error)

	// Channels for non-blocking ingestion
	imageCh chan PendingImage

	// Deferred processing scheduler
	embedHour   int
	embedMinute int
	persistDir string
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

const persistFile = "deferred.json"

func NewBatchIndexer(embedHour, embedMinute int) *BatchIndexer {
	b := &BatchIndexer{
		msgBuffer:   make(map[string]*messageBuffer),
		stopCh:      make(chan struct{}),
		imageCh:     make(chan PendingImage, 1000),
		embedHour:   embedHour,
		embedMinute: embedMinute,
		persistDir:  "./bot-data",
	}

	// Load persisted deferred data
	b.loadDeferred()

	// Start ingestion goroutine
	b.wg.Add(1)
	go b.ingestLoop()
	// Start deferred processing scheduler
	b.wg.Add(1)
	go b.deferredProcessingLoop()
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
			return // No persisted data
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

	log.Printf("INFO batch_indexer: loaded %d deferred images and %d deferred text embeddings", len(data.DeferredImages), len(data.DeferredTextEmbed))
}

// OnTextMessage indexes text immediately (no batching).
func (b *BatchIndexer) OnTextMessage(msg PendingMessage) {
	b.indexTextMessage(msg)

	// Queue for deferred embedding (with dedup)
	b.mu.Lock()
	if !b.hasDeferredText(msg.EventID) {
		b.deferredTextEmbed = append(b.deferredTextEmbed, msg)
	}
	b.mu.Unlock()
	b.saveDeferred()
}

// bufferKey creates a unique key for the message buffer based on room and user.
func bufferKey(roomID, userID string) string {
	return roomID + "|" + userID
}

// OnTextMessageWithBuffering accumulates consecutive messages from the same user in the same room.
// Each (roomID, userID) pair has its own independent buffer.
// First message: index immediately. Second message from same key: append text, re-index with first event ID (overwrites).
func (b *BatchIndexer) OnTextMessageWithBuffering(msg PendingMessage) {
	b.mu.Lock()
	
	key := bufferKey(msg.RoomID, msg.UserID)
	
	if existing, exists := b.msgBuffer[key]; exists {
		// Same (room, user) — append text and re-index with first event ID (overwrites old doc)
		existing.text = existing.text + "\n" + msg.Text
		existing.Timestamp = msg.Timestamp
		b.reindexBufferLocked(existing)
		delete(b.msgBuffer, key)
	} else {
		// New (room, user) pair — create buffer
		b.msgBuffer[key] = &messageBuffer{
			firstEventID: msg.EventID,
			text:         msg.Text,
			RoomID:       msg.RoomID,
			UserID:       msg.UserID,
			Timestamp:    msg.Timestamp,
		}
		// Index immediately (first message)
		b.flushBufferLocked(b.msgBuffer[key])
	}
	
	b.mu.Unlock()
	b.saveDeferred()
}

// flushBufferLocked indexes the accumulated buffer and queues it for deferred embedding.
// Must be called with b.mu held. Does NOT call saveDeferred — caller must do that after unlocking.
func (b *BatchIndexer) flushBufferLocked(buf *messageBuffer) {
	// Create a PendingMessage with the accumulated text and first event ID
	msg := PendingMessage{
		EventID:   buf.firstEventID,
		RoomID:    buf.RoomID,
		UserID:    buf.UserID,
		Timestamp: buf.Timestamp,
		EventType: "m.room.message",
		Text:      buf.text,
	}
	b.indexTextMessage(msg)

	// Queue for deferred embedding
	if !b.hasDeferredText(buf.firstEventID) {
		b.deferredTextEmbed = append(b.deferredTextEmbed, msg)
	}
}

// reindexBufferLocked re-indexes the accumulated buffer with the same event ID (overwrites old doc).
// Skips dedup check since we're replacing an existing document.
// Must be called with b.mu held. Does NOT call saveDeferred — caller must do that after unlocking.
func (b *BatchIndexer) reindexBufferLocked(buf *messageBuffer) {
	// Create a PendingMessage with the accumulated text and first event ID
	msg := PendingMessage{
		EventID:   buf.firstEventID,
		RoomID:    buf.RoomID,
		UserID:    buf.UserID,
		Timestamp: buf.Timestamp,
		EventType: "m.room.message",
		Text:      buf.text,
	}

	// Re-index directly (skip dedup check — we're overwriting)
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
		log.Printf("ERROR batch_indexer: reindex error for event %s: %v", msg.EventID, err)
	} else {
		log.Printf("INFO batch_indexer: re-indexed text event %s (%d lines)", msg.EventID, strings.Count(msg.Text, "\n")+1)
	}

	// Update deferred embedding queue — remove old entry and add new
	for i, pending := range b.deferredTextEmbed {
		if pending.EventID == buf.firstEventID {
			b.deferredTextEmbed = append(b.deferredTextEmbed[:i], b.deferredTextEmbed[i+1:]...)
			break
		}
	}
	b.deferredTextEmbed = append(b.deferredTextEmbed, msg)
}

// OnImageMessage is non-blocking — enqueues image via channel (with dedup).
func (b *BatchIndexer) OnImageMessage(img PendingImage) {
	select {
	case b.imageCh <- img:
	default:
	}
}

func (b *BatchIndexer) ingestLoop() {
	defer b.wg.Done()
	for {
		select {
		case img := <-b.imageCh:
			b.mu.Lock()
			if !b.hasDeferredImage(img.EventID) {
				b.deferredImages = append(b.deferredImages, img)
			}
			b.mu.Unlock()
			b.saveDeferred()

		case <-b.stopCh:
			return
		}
	}
}

// hasDeferredImage checks if an image with the given EventID is already in deferredImages.
func (b *BatchIndexer) hasDeferredImage(eventID string) bool {
	for _, img := range b.deferredImages {
		if img.EventID == eventID {
			return true
		}
	}
	return false
}

// hasDeferredText checks if a text message with the given EventID is already in deferredTextEmbed.
func (b *BatchIndexer) hasDeferredText(eventID string) bool {
	for _, msg := range b.deferredTextEmbed {
		if msg.EventID == eventID {
			return true
		}
	}
	return false
}

// deferredProcessingLoop waits until the configured daily time and processes
// all deferred images (VLM description + embedding) in one batch.
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
			// Copy all deferred items for processing
			deferredImages := make([]PendingImage, len(b.deferredImages))
			copy(deferredImages, b.deferredImages)
			deferredText := make([]PendingMessage, len(b.deferredTextEmbed))
			copy(deferredText, b.deferredTextEmbed)
			b.mu.Unlock()

			var failedImages []PendingImage
			var failedText []PendingMessage

			// Process deferred images
			if len(deferredImages) > 0 {
				log.Printf("INFO batch_indexer: processing %d deferred images", len(deferredImages))
				if b.ProcessDeferredFn != nil {
					var err error
					failedImages, err = b.ProcessDeferredFn(deferredImages)
					if err != nil {
						log.Printf("ERROR batch_indexer: deferred images processing error: %v", err)
					}
				}
			}

			// Process deferred text embeddings
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

			// Update queues and save once
			b.mu.Lock()
			b.deferredImages = failedImages
			b.deferredTextEmbed = failedText
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
			log.Printf("INFO batch_indexer: skipping already indexed event %s", msg.EventID)
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
	} else {
		log.Printf("INFO batch_indexer: indexed text event %s", msg.EventID)
	}
}
