package indexer

import (
	"maps"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type TaskType int

const (
	TaskTextIndex TaskType = iota
	TaskImageQueue
	TaskFlush
	TaskGetLastUsedRoom
	TaskSetLastUsedRoom
)

type IndexTask struct {
	Type   TaskType
	Batch  bool // if true — no flush after indexing (accumulate batch for history scan)

	TextMsg    PendingMessage
	ImageMsg   PendingImage
	RoomID     string
	DMRoomID   string
	LastRoomID string

	RespCh chan TaskResponse // for synchronous operations
}

type TaskResponse struct {
	IndexedCount int
	LastUsedRoom string
}

// DeferredTask carries deferred images and texts to deferred-worker.
type DeferredTask struct {
	Images     []PendingImage
	Texts      []PendingMessage
	ImageIDs   map[string]bool // EventIDs of images in this copy
	TextIDs    map[string]bool // EventIDs of texts in this copy
}

// DeferredResponse carries back the failed items from deferred processing.
type DeferredResponse struct {
	FailedImages []PendingImage
	FailedTexts  []PendingMessage
	ImageIDs     map[string]bool // EventIDs of images in the original copy
	TextIDs      map[string]bool // EventIDs of texts in the original copy
}

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
	taskCh  chan IndexTask
	stopCh  chan struct{}
	wg      sync.WaitGroup

	// State — modified ONLY by taskWorker (no mutexes!)
	msgBuffer         map[string]*messageBuffer
	lastUsedRooms     map[string]string
	deferredImages    []PendingImage
	deferredTextEmbed []PendingMessage

	// Deferred-worker — separate goroutine that processes deferred items.
	// taskWorker owns the deferred state. It copies it and sends via deferredCh,
	// then waits for DeferredResponse via defRespCh.
	// CSP pattern: no shared mutable state, no mutexes.
	deferredCh     chan DeferredTask
	defRespCh      chan DeferredResponse
	deferredWg     sync.WaitGroup
	deferredActive bool // set by taskWorker while deferred is active; checked by taskWorker for scheduling

	// Timers — per-instance
	deferredTimerCh chan struct{}
	saveTimerCh     chan struct{}

	bleveClient BleveClientInterface
	embedClient EmbedClientInterface
	imageProc   *ImageProcessor

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

func NewBatchIndexer(embedHour, embedMinute int, persistDir string, bleveClient BleveClientInterface, embedClient EmbedClientInterface, imageProc *ImageProcessor) *BatchIndexer {
	b := &BatchIndexer{
		taskCh:      make(chan IndexTask, 1000),
		stopCh:      make(chan struct{}),
		msgBuffer:   make(map[string]*messageBuffer),
		embedHour:   embedHour,
		embedMinute: embedMinute,
		persistDir:  persistDir,

		bleveClient: bleveClient,
		embedClient: embedClient,
		imageProc:   imageProc,
	}

	b.loadDeferred()

	b.wg.Add(1)
	go b.taskWorker()
	return b
}

type persistData struct {
	DeferredImages    []PendingImage   `json:"deferred_images"`
	DeferredTextEmbed []PendingMessage `json:"deferred_text"`
	LastUsedRooms     map[string]string `json:"last_used_rooms,omitempty"`
}

func (b *BatchIndexer) saveDeferred() {
	data := persistData{
		DeferredImages:    make([]PendingImage, len(b.deferredImages)),
		DeferredTextEmbed: make([]PendingMessage, len(b.deferredTextEmbed)),
	}
	copy(data.DeferredImages, b.deferredImages)
	copy(data.DeferredTextEmbed, b.deferredTextEmbed)
	if len(b.lastUsedRooms) > 0 {
		data.LastUsedRooms = make(map[string]string, len(b.lastUsedRooms))
		maps.Copy(data.LastUsedRooms, b.lastUsedRooms)
	}

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

	b.deferredImages = append(b.deferredImages, data.DeferredImages...)
	b.deferredTextEmbed = append(b.deferredTextEmbed, data.DeferredTextEmbed...)
	if len(data.LastUsedRooms) > 0 {
		b.lastUsedRooms = data.LastUsedRooms
	}

	log.Printf("INFO batch_indexer: loaded %d deferred images, %d deferred text embeddings", len(data.DeferredImages), len(data.DeferredTextEmbed))
}

// GetLastUsedRoom returns the last room used for search in the given DM room.
func (b *BatchIndexer) GetLastUsedRoom(dmRoomID string) string {
	respCh := make(chan TaskResponse, 1)
	b.taskCh <- IndexTask{
		Type:     TaskGetLastUsedRoom,
		DMRoomID: dmRoomID,
		RespCh:   respCh,
	}
	resp := <-respCh
	return resp.LastUsedRoom
}

// SetLastUsedRoom records the last room used for search in the given DM room.
// Only writes to disk if the value actually changed.
func (b *BatchIndexer) SetLastUsedRoom(dmRoomID, roomID string) {
	b.taskCh <- IndexTask{
		Type:       TaskSetLastUsedRoom,
		DMRoomID:   dmRoomID,
		LastRoomID: roomID,
	}
}

// OnTextMessageWithBuffering accumulates consecutive messages from the same user in the same room.
// Buffer is keyed by roomID. When a different user writes in the same room, the previous buffer is flushed.
// Same user writing again: appends to existing buffer, re-indexes.
// If batch is true, dedup check is performed and messages are batched.
// If batch is false, dedup check is skipped so live messages can re-index with updated text (e.g. link previews).
// Returns the number of documents actually indexed during this call.
func (b *BatchIndexer) OnTextMessageWithBuffering(msg PendingMessage, batch bool) int {
	respCh := make(chan TaskResponse, 1)
	b.taskCh <- IndexTask{
		Type:    TaskTextIndex,
		Batch:   batch, // false = live (no dedup), true = batch (with dedup)
		TextMsg: msg,
		RespCh:  respCh,
	}
	resp := <-respCh
	return resp.IndexedCount
}

// OnImageMessage enqueues image via taskCh (non-blocking).
func (b *BatchIndexer) OnImageMessage(img PendingImage) {
	select {
	case b.taskCh <- IndexTask{
		Type:     TaskImageQueue,
		ImageMsg: img,
	}:
	default:
	}
}

// QueueImage synchronously enqueues an image for deferred processing.
// Returns true if the image was added (false if already indexed).
// Used during history scan where sync dedup check is needed.
func (b *BatchIndexer) QueueImage(img PendingImage) bool {
	respCh := make(chan TaskResponse, 1)
	b.taskCh <- IndexTask{
		Type:     TaskImageQueue,
		ImageMsg: img,
		RespCh:   respCh,
	}
	return (<-respCh).IndexedCount > 0
}

// handleTask dispatches a task to the appropriate handler.
func (b *BatchIndexer) handleTask(task IndexTask) {
	switch task.Type {
	case TaskTextIndex:
		b.handleTextIndex(task)
	case TaskImageQueue:
		b.handleImageQueue(task)
	case TaskFlush:
		b.handleFlush(task)
	case TaskGetLastUsedRoom:
		b.handleGetLastUsedRoom(task)
	case TaskSetLastUsedRoom:
		b.handleSetLastUsedRoom(task)
	}
}

// handleTextIndex indexes a text message.
func (b *BatchIndexer) handleTextIndex(task IndexTask) {
	msg := task.TextMsg
	batch := task.Batch

	// Dedup check — skip for live messages (batch=false) so they can re-index with updated text (e.g. link previews)
	if batch {
		indexed, err := b.bleveClient.IsEventIDExists(msg.EventID)
		if err != nil {
			log.Printf("ERROR batch_indexer: IsEventIDExists error for event %s: %v", msg.EventID, err)
		}
		if indexed {
			if task.RespCh != nil {
				task.RespCh <- TaskResponse{IndexedCount: 0}
			}
			return
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

	if err := b.bleveClient.IndexDocumentStruct(doc); err != nil {
		log.Printf("ERROR batch_indexer: IndexDocumentStruct error for event %s: %v", msg.EventID, err)
		if task.RespCh != nil {
			task.RespCh <- TaskResponse{IndexedCount: 0}
		}
		return
	}

	// Add event ID and flush eventID batch for dedup
	if err := b.bleveClient.AddEventID(msg.EventID); err != nil {
		log.Printf("WARN batch_indexer: failed to add eventID %s", msg.EventID)
	}

	// Flush for live messages (batch=false) so documents appear in search immediately.
	// For history scan (batch=true), documents accumulate in the batch buffer
	// and are flushed later by explicit Flush() call.
	if !batch {
		if err := b.bleveClient.Flush(); err != nil {
			log.Printf("WARN batch_indexer: failed to flush document batch after live index: %v", err)
		}
		if err := b.bleveClient.FlushEventID(); err != nil {
			log.Printf("WARN batch_indexer: failed to flush eventID batch after live index: %v", err)
		}
	}

	log.Printf("INFO batch_indexer: indexed text event %s", msg.EventID)
	b.deferredTextEmbed = append(b.deferredTextEmbed, msg)

	if task.RespCh != nil {
		task.RespCh <- TaskResponse{IndexedCount: 1}
	}
}

// handleImageQueue adds an image to the deferred queue.
func (b *BatchIndexer) handleImageQueue(task IndexTask) {
	img := task.ImageMsg

	if indexed, err := b.bleveClient.IsEventIDExists(img.EventID); err == nil && indexed {
		if task.RespCh != nil {
			task.RespCh <- TaskResponse{IndexedCount: 0} // already indexed
		}
		return
	}

	b.deferredImages = append(b.deferredImages, img)

	if err := b.bleveClient.AddEventID(img.EventID); err != nil {
		log.Printf("WARN batch_indexer: failed to add eventID %s", img.EventID)
	}

	if task.RespCh != nil {
		task.RespCh <- TaskResponse{IndexedCount: 1}
	}
}

// handleFlush flushes message buffers for a specific room or all rooms.
func (b *BatchIndexer) handleFlush(task IndexTask) {
	var count int
	if task.RoomID != "" {
		// Flush single room buffer
		if buf, exists := b.msgBuffer[task.RoomID]; exists {
			count = b.flushBuffer(buf)
			delete(b.msgBuffer, task.RoomID)
		}
	} else {
		// Flush all rooms
		for roomID, buf := range b.msgBuffer {
			count += b.flushBuffer(buf)
			delete(b.msgBuffer, roomID)
		}
	}

	if task.RespCh != nil {
		task.RespCh <- TaskResponse{IndexedCount: count}
	}
}

// handleGetLastUsedRoom returns the last room used for search in a DM room.
func (b *BatchIndexer) handleGetLastUsedRoom(task IndexTask) {
	var room string
	if b.lastUsedRooms != nil {
		room = b.lastUsedRooms[task.DMRoomID]
	}
	task.RespCh <- TaskResponse{LastUsedRoom: room}
}

// handleSetLastUsedRoom records the last room used for search in a DM room.
func (b *BatchIndexer) handleSetLastUsedRoom(task IndexTask) {
	if b.lastUsedRooms == nil {
		b.lastUsedRooms = make(map[string]string)
	}
	if b.lastUsedRooms[task.DMRoomID] == task.LastRoomID {
		return
	}
	b.lastUsedRooms[task.DMRoomID] = task.LastRoomID
	b.saveDeferred()
}

// flushBuffer indexes the accumulated buffer.
// Returns 1 if the document was indexed, 0 if skipped (already indexed).
func (b *BatchIndexer) flushBuffer(buf *messageBuffer) int {
	msg := PendingMessage{
		EventID:   buf.firstEventID,
		RoomID:    buf.RoomID,
		UserID:    buf.UserID,
		Timestamp: buf.Timestamp,
		EventType: "m.room.message",
		Text:      buf.text,
	}

	// Dedup check
	indexed, err := b.bleveClient.IsEventIDExists(msg.EventID)
	if err == nil && indexed {
		return 0
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

	if err := b.bleveClient.IndexDocumentStruct(doc); err != nil {
		log.Printf("ERROR batch_indexer: IndexDocumentStruct error for event %s: %v", msg.EventID, err)
		return 0
	}
	log.Printf("INFO batch_indexer: indexed text event %s", msg.EventID)
	b.deferredTextEmbed = append(b.deferredTextEmbed, msg)

	if err := b.bleveClient.AddEventID(msg.EventID); err != nil {
		log.Printf("WARN batch_indexer: failed to add eventID %s", msg.EventID)
	}

	// Flush after buffer flush — this is always history scan, documents must appear
	if err := b.bleveClient.Flush(); err != nil {
		log.Printf("WARN batch_indexer: failed to flush document batch after buffer flush: %v", err)
	}
	if err := b.bleveClient.FlushEventID(); err != nil {
		log.Printf("WARN batch_indexer: failed to flush eventID batch after buffer flush: %v", err)
	}

	return 1
}

// taskWorker is the single goroutine that processes all tasks.
// Since it's the only goroutine, no mutexes are needed for local state.
// Deferred processing is handled by a separate deferred-worker goroutine.
func (b *BatchIndexer) taskWorker() {
	defer b.wg.Done()

	// Start timer for deferred processing
	b.startDeferredTimer()
	// Start periodic save timer
	b.startPeriodicSaveTimer()
	// Start deferred-worker goroutine
	b.deferredCh = make(chan DeferredTask, 1)
	b.defRespCh = make(chan DeferredResponse, 1)
	b.deferredWg.Add(1)
	go b.deferredWorker()

	for {
		select {
		case task := <-b.taskCh:
			b.handleTask(task)

			// After processing a task, check if deferred processing is due
			// and not already active (no duplicate schedules)
			if b.shouldProcessDeferred() && !b.deferredActive {
				b.deferredActive = true
				b.sendDeferredTask()
			}

		case resp := <-b.defRespCh:
			b.processDeferredResponse(resp)

		case <-b.deferredTimerCh:
			// Only schedule if not already active
			if !b.deferredActive {
				b.deferredActive = true
				b.sendDeferredTask()
			}

		case <-b.saveTimerCh:
			b.saveDeferred()

		case <-b.stopCh:
			// Drain remaining tasks
		drainLoop:
			for {
				select {
				case task := <-b.taskCh:
					b.handleTask(task)
				default:
					break drainLoop
				}
			}
			// Final deferred processing
			if b.shouldProcessDeferred() {
				b.processDeferred()
			}
			b.saveDeferred()
			return
		}
	}
}

// deferredWorker is the single goroutine that processes deferred items.
// It reads DeferredTask from deferredCh, processes all items (VLM describe,
// embedding, indexing), and sends failed items via defRespCh.
func (b *BatchIndexer) deferredWorker() {
	defer b.deferredWg.Done()
	for {
		select {
		case dt := <-b.deferredCh:
			b.processDeferredItems(dt)
		case <-b.stopCh:
			return
		}
	}
}

// shouldProcessDeferred checks if it's time to process deferred items.
func (b *BatchIndexer) shouldProcessDeferred() bool {
	now := time.Now()
	next := time.Date(now.Year(), now.Month(), now.Day(),
		b.embedHour, b.embedMinute, 0, 0, now.Location())
	if next.Before(now) || next.Equal(now) {
		next = next.Add(24 * time.Hour)
	}
	return now.After(next)
}

// processDeferredItems is called by deferred-worker to process all deferred items.
// It performs VLM describe, embedding creation, and indexing for images and texts.
// Sends failed items back via defRespCh.
func (b *BatchIndexer) processDeferredItems(dt DeferredTask) {
	deferredImages := dt.Images
	deferredText := dt.Texts

	var failedImages []PendingImage
	var failedText []PendingMessage

	if len(deferredImages) > 0 {
		log.Printf("INFO batch_indexer: phase 1 - describing %d deferred images", len(deferredImages))
		failedImages = b.processImageDesc(deferredImages)

		log.Printf("INFO batch_indexer: phase 2 - embedding %d image descriptions", len(deferredImages)-len(failedImages))
		failedImages = b.processImageEmbed(deferredImages, failedImages)
	}

	if len(deferredText) > 0 {
		log.Printf("INFO batch_indexer: processing %d deferred text embeddings", len(deferredText))
		failedText = b.processDeferredText(deferredText)
	}

	// Send failed items back to taskWorker
	b.defRespCh <- DeferredResponse{
		FailedImages: failedImages,
		FailedTexts:  failedText,
		ImageIDs:     dt.ImageIDs,
		TextIDs:      dt.TextIDs,
	}
}

// sendDeferredTask copies deferred state and sends it to deferred-worker.
// Called by taskWorker — it owns the deferred state, so no mutex needed.
func (b *BatchIndexer) sendDeferredTask() {
	deferredImages := make([]PendingImage, len(b.deferredImages))
	copy(deferredImages, b.deferredImages)
	deferredText := make([]PendingMessage, len(b.deferredTextEmbed))
	copy(deferredText, b.deferredTextEmbed)

	// Track which EventIDs are in this copy so cleanup doesn't remove new items
	imageIDs := make(map[string]bool, len(b.deferredImages))
	for _, img := range b.deferredImages {
		imageIDs[img.EventID] = true
	}
	textIDs := make(map[string]bool, len(b.deferredTextEmbed))
	for _, msg := range b.deferredTextEmbed {
		textIDs[msg.EventID] = true
	}

	select {
	case b.deferredCh <- DeferredTask{
		Images:   deferredImages,
		Texts:    deferredText,
		ImageIDs: imageIDs,
		TextIDs:  textIDs,
	}:
	default:
		// Already in progress, skip
		b.deferredActive = false
	}
}

// processDeferredResponse handles the DeferredResponse from deferred-worker.
// Called by taskWorker — it owns the deferred state, so no mutex needed.
func (b *BatchIndexer) processDeferredResponse(resp DeferredResponse) {
	b.deferredActive = false

	// Clean up successful images from deferred state.
	// Only remove items that were in the original copy (resp.ImageIDs).
	// New items added after the copy must NOT be removed.
	successImageIDs := make(map[string]bool)
	for _, img := range resp.FailedImages {
		successImageIDs[img.EventID] = false
	}
	for _, img := range b.deferredImages {
		if _, inCopy := resp.ImageIDs[img.EventID]; !inCopy {
			continue // skip items added after the copy
		}
		if _, ok := successImageIDs[img.EventID]; !ok {
			successImageIDs[img.EventID] = true
		}
	}
	for i := 0; i < len(b.deferredImages); {
		if successImageIDs[b.deferredImages[i].EventID] {
			b.deferredImages = append(b.deferredImages[:i], b.deferredImages[i+1:]...)
		} else {
			i++
		}
	}

	// Clean up successful texts from deferred state.
	// Only remove items that were in the original copy (resp.TextIDs).
	successTextIDs := make(map[string]bool)
	for _, msg := range resp.FailedTexts {
		successTextIDs[msg.EventID] = false
	}
	for _, msg := range b.deferredTextEmbed {
		if _, inCopy := resp.TextIDs[msg.EventID]; !inCopy {
			continue // skip items added after the copy
		}
		if _, ok := successTextIDs[msg.EventID]; !ok {
			successTextIDs[msg.EventID] = true
		}
	}
	for i := 0; i < len(b.deferredTextEmbed); {
		if successTextIDs[b.deferredTextEmbed[i].EventID] {
			b.deferredTextEmbed = append(b.deferredTextEmbed[:i], b.deferredTextEmbed[i+1:]...)
		} else {
			i++
		}
	}

	// Save deferred state after cleanup
	b.saveDeferred()

	// Reset timer
	b.startDeferredTimer()
}

// processDeferred is called from Stop() — taskWorker has finished,
// so we can safely copy and process without concurrent writes.
func (b *BatchIndexer) processDeferred() {
	defer b.deferredWg.Done()
	defer func() { b.deferredActive = false }()

	// taskWorker is done, no concurrent writes
	deferredImages := make([]PendingImage, len(b.deferredImages))
	copy(deferredImages, b.deferredImages)
	deferredText := make([]PendingMessage, len(b.deferredTextEmbed))
	copy(deferredText, b.deferredTextEmbed)

	// Track which EventIDs are in this copy
	imageIDs := make(map[string]bool, len(b.deferredImages))
	for _, img := range b.deferredImages {
		imageIDs[img.EventID] = true
	}
	textIDs := make(map[string]bool, len(b.deferredTextEmbed))
	for _, msg := range b.deferredTextEmbed {
		textIDs[msg.EventID] = true
	}

	b.deferredCh <- DeferredTask{
		Images:   deferredImages,
		Texts:    deferredText,
		ImageIDs: imageIDs,
		TextIDs:  textIDs,
	}

	// Wait for response (synchronous since we're shutting down)
	resp := <-b.defRespCh

	// Clean up successful images from deferred state.
	// Only remove items that were in the original copy (resp.ImageIDs).
	successImageIDs := make(map[string]bool)
	for _, img := range resp.FailedImages {
		successImageIDs[img.EventID] = false
	}
	for _, img := range deferredImages {
		if _, inCopy := resp.ImageIDs[img.EventID]; !inCopy {
			continue // skip items added after the copy
		}
		if _, ok := successImageIDs[img.EventID]; !ok {
			successImageIDs[img.EventID] = true
		}
	}
	for i := 0; i < len(b.deferredImages); {
		if successImageIDs[b.deferredImages[i].EventID] {
			b.deferredImages = append(b.deferredImages[:i], b.deferredImages[i+1:]...)
		} else {
			i++
		}
	}

	// Clean up successful texts from deferred state.
	// Only remove items that were in the original copy (resp.TextIDs).
	successTextIDs := make(map[string]bool)
	for _, msg := range resp.FailedTexts {
		successTextIDs[msg.EventID] = false
	}
	for _, msg := range deferredText {
		if _, inCopy := resp.TextIDs[msg.EventID]; !inCopy {
			continue // skip items added after the copy
		}
		if _, ok := successTextIDs[msg.EventID]; !ok {
			successTextIDs[msg.EventID] = true
		}
	}
	for i := 0; i < len(b.deferredTextEmbed); {
		if successTextIDs[b.deferredTextEmbed[i].EventID] {
			b.deferredTextEmbed = append(b.deferredTextEmbed[:i], b.deferredTextEmbed[i+1:]...)
		} else {
			i++
		}
	}

	// Save deferred state after cleanup
	b.saveDeferred()

	// Reset timer
	b.startDeferredTimer()
}

// processImageDesc describes deferred images using VLM.
func (b *BatchIndexer) processImageDesc(images []PendingImage) []PendingImage {
	var failed []PendingImage
	for i, img := range images {
		desc, err := b.imageProc.DescribeImageOnly(img.RawURL, img.Timestamp)
		if err != nil {
			log.Printf("ERROR batch_indexer: failed to describe deferred image %s: %v", img.EventID, err)
			failed = append(failed, img)
			continue
		}
		images[i].Description = desc
	}
	return failed
}

// processImageEmbed creates embeddings for image descriptions.
func (b *BatchIndexer) processImageEmbed(images []PendingImage, failedImages []PendingImage) []PendingImage {
	var failed []PendingImage
	failedMap := make(map[string]bool)
	for _, img := range failedImages {
		failedMap[img.EventID] = true
	}

	for _, img := range images {
		if _, ok := failedMap[img.EventID]; ok {
			failed = append(failed, img)
			continue
		}
		if img.Description == "" {
			failed = append(failed, img)
			continue
		}

		vector, err := b.embedClient.CreateEmbedding(img.Description, "search_document: ")
		if err != nil {
			log.Printf("ERROR batch_indexer: failed to embed deferred image %s: %v", img.EventID, err)
			failed = append(failed, img)
			continue
		}

		doc := IndexedDocument{
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

		if err := b.bleveClient.IndexDocumentStruct(doc); err != nil {
			log.Printf("ERROR batch_indexer: failed to index deferred image %s: %v", img.EventID, err)
			failed = append(failed, img)
			continue
		}
		if err := b.bleveClient.AddEventID(img.EventID); err != nil {
			log.Printf("WARN batch_indexer: failed to add eventID %s", img.EventID)
		}
	}
	// Flush deferred image embeddings so search can find them
	if err := b.bleveClient.Flush(); err != nil {
		log.Printf("WARN batch_indexer: failed to flush document batch after deferred image: %v", err)
	}
	if err := b.bleveClient.FlushEventID(); err != nil {
		log.Printf("WARN batch_indexer: failed to flush eventID batch after deferred image: %v", err)
	}
	return failed
}

// processDeferredText creates embeddings for text messages.
func (b *BatchIndexer) processDeferredText(texts []PendingMessage) []PendingMessage {
	var failed []PendingMessage
	for _, msg := range texts {
		vector, err := b.embedClient.CreateEmbedding(msg.Text, "search_document: ")
		if err != nil {
			log.Printf("ERROR batch_indexer: failed to embed deferred text %s: %v", msg.EventID, err)
			failed = append(failed, msg)
			continue
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

		if err := b.bleveClient.IndexDocumentStruct(doc); err != nil {
			log.Printf("ERROR batch_indexer: failed to index deferred text %s: %v", msg.EventID, err)
			failed = append(failed, msg)
			continue
		}
		if err := b.bleveClient.AddEventID(msg.EventID); err != nil {
			log.Printf("WARN batch_indexer: failed to add eventID %s", msg.EventID)
		}
	}
	// Flush deferred text embeddings so search can find them
	if err := b.bleveClient.Flush(); err != nil {
		log.Printf("WARN batch_indexer: failed to flush document batch after deferred text: %v", err)
	}
	if err := b.bleveClient.FlushEventID(); err != nil {
		log.Printf("WARN batch_indexer: failed to flush eventID batch after deferred text: %v", err)
	}
	return failed
}

// startDeferredTimer starts a timer for deferred processing.
func (b *BatchIndexer) startDeferredTimer() {
	now := time.Now()
	next := time.Date(now.Year(), now.Month(), now.Day(),
		b.embedHour, b.embedMinute, 0, 0, now.Location())
	if next.Before(now) || next.Equal(now) {
		next = next.Add(24 * time.Hour)
	}
	b.deferredTimerCh = make(chan struct{}, 1)
	go func() {
		select {
		case <-time.After(time.Until(next)):
			select {
			case b.deferredTimerCh <- struct{}{}:
			default:
			}
		case <-b.stopCh:
		}
	}()
}

// startPeriodicSaveTimer starts a 1-minute timer for periodic saves.
func (b *BatchIndexer) startPeriodicSaveTimer() {
	b.saveTimerCh = make(chan struct{}, 1)
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				select {
				case b.saveTimerCh <- struct{}{}:
				default:
				}
			case <-b.stopCh:
				return
			}
		}
	}()
}

// Stop shuts down the batch indexer.
// Drains remaining tasks before exiting to ensure in-flight indexing completes.
func (b *BatchIndexer) Stop() {
	close(b.stopCh)
	b.wg.Wait()
	b.deferredWg.Wait()
	b.saveDeferred()
}

// FlushRoom flushes the buffer for a specific room.
// Returns the number of documents actually indexed (not skipped).
func (b *BatchIndexer) FlushRoom(roomID string) int {
	respCh := make(chan TaskResponse, 1)
	b.taskCh <- IndexTask{
		Type:   TaskFlush,
		RoomID: roomID,
		RespCh: respCh,
	}
	resp := <-respCh
	return resp.IndexedCount
}

// FlushBufferedMessages flushes all pending message buffers by iterating
// the msgBuffer map and calling flushBuffer for each entry.
// Returns the count of documents that were actually indexed (not skipped).
func (b *BatchIndexer) FlushBufferedMessages() int {
	// Get room keys under taskCh channel (single goroutine, no mutex needed)
	roomKeys := make([]string, 0, len(b.msgBuffer))
	for roomID := range b.msgBuffer {
		roomKeys = append(roomKeys, roomID)
	}

	flushed := 0
	for _, roomID := range roomKeys {
		flushed += b.FlushRoom(roomID)
	}
	return flushed
}
