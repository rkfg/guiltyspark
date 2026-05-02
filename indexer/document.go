package indexer

type IndexedDocument struct {
	ID        string    `json:"id"`
	EventID   string    `json:"event_id"`
	RoomID    string    `json:"room_id"`
	UserID    string    `json:"user_id"`
	Timestamp int64     `json:"timestamp"`
	EventType string    `json:"event_type"`
	Text      string    `json:"text"`
	ImageDesc string    `json:"image_desc"`
	Vector    []float32 `json:"vector"`
	RawURL    string    `json:"raw_url"`
	FileName  string    `json:"file_name"`
	MimeType  string    `json:"mime_type"`
}
