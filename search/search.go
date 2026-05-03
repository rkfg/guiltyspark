package search

import (
	"fmt"
	"log"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/rkfg/guiltyspark/config"
	"github.com/rkfg/guiltyspark/indexer"
)

type Result struct {
	EventID   string
	RoomID    string
	UserID    string
	Timestamp int64
	Text      string
	ImageDesc string
	RawURL    string
	FileName  string
	MimeType  string
	Score     float64
	Type      string // "text" or "image"
}

type SearchResult struct {
	Exact       []Result
	Semantic    []Result
	Query       string
	QueryVector []float32
}

type Engine struct {
	bleveClient *indexer.BleveClient
	embedClient interface {
		CreateEmbedding(text string) ([]float32, error)
	}
	cfg *config.SearchConfig
}

func NewEngine(bleveClient *indexer.BleveClient, embedClient interface {
	CreateEmbedding(text string) ([]float32, error)
}, cfg *config.SearchConfig) *Engine {
	return &Engine{
		bleveClient: bleveClient,
		embedClient: embedClient,
		cfg:         cfg,
	}
}

// russianStopWords contains common Russian stop words that should be filtered
var russianStopWords = map[string]bool{
	"и": true, "в": true, "на": true, "не": true, "что": true, "это": true, "с": true, "по": true,
	"к": true, "у": true, "из": true, "но": true, "или": true, "как": true, "все": true, "так": true,
	"уже": true, "ещё": true, "то": true, "за": true, "бы": true, "от": true, "до": true, "о": true,
	"для": true, "же": true, "ли": true, "ни": true, "быть": true, "он": true, "она": true, "оно": true,
	"они": true, "мы": true, "вы": true, "я": true, "ты": true, "а": true, "нет": true, "да": true,
	"тот": true, "эта": true, "при": true, "между": true, "всё": true, "там": true, "тут": true,
	// English common stop words
	"the": true, "and": true, "or": true, "but": true, "in": true, "on": true, "at": true, "to": true,
	"for": true, "of": true, "with": true, "by": true, "is": true, "it": true, "this": true, "that": true,
	"are": true, "was": true, "were": true, "be": true, "has": true, "have": true, "had": true,
	"do": true, "does": true, "did": true, "will": true, "would": true, "can": true, "could": true,
	"should": true, "may": true, "might": true, "not": true, "no": true, "from": true, "as": true,
	"an": true, "if": true, "so": true, "up": true, "out": true, "into": true, "than": true, "then": true,
}

func filterStopWords(query string) string {
	words := strings.Fields(strings.ToLower(query))
	var filtered []string
	for _, w := range words {
		// Clean punctuation
		clean := strings.Trim(w, ".,!?;:\"'()[]{}")
		if clean != "" && !russianStopWords[clean] {
			filtered = append(filtered, clean)
		}
	}
	return strings.Join(filtered, " ")
}

func (e *Engine) Search(queryText string, roomFilter, userFilter string) (*SearchResult, error) {
	result := &SearchResult{Query: queryText}

	// Filter stop words from query
	filteredQuery := filterStopWords(queryText)
	if filteredQuery == "" {
		return result, nil
	}

	// Get embedding for semantic search
	vector, err := e.embedClient.CreateEmbedding(filteredQuery)
	if err != nil {
		return nil, fmt.Errorf("create query embedding: %w", err)
	}
	result.QueryVector = vector
	log.Printf("INFO search: query=%q filtered=%q embedding dims=%d first3=%v", queryText, filteredQuery, len(vector), vector[:min(3, len(vector))])

	// Exact search
	exactResults, err := e.bleveClient.SearchExact(filteredQuery, roomFilter)
	if err != nil {
		return nil, fmt.Errorf("exact search: %w", err)
	}

	for _, hit := range exactResults.Hits {
		if len(userFilter) > 0 && hit.Fields["user_id"] != userFilter {
			continue
		}
		r := convertHit(hit)
		r.Score = hit.Score
		result.Exact = append(result.Exact, r)
	}

	// Semantic search using Bleve native kNN with FAISS backend
	semanticResults, err := e.bleveClient.SearchSemantic(vector, roomFilter)
	if err != nil {
		return nil, fmt.Errorf("semantic search: %w", err)
	}

	log.Printf("INFO search: semantic total=%d (limit=%d)", len(semanticResults.Hits), e.cfg.ResultLimit)
	for i, hit := range semanticResults.Hits {
		log.Printf("INFO search:   semantic hit[%d] id=%s score=%.6f", i, hit.ID, hit.Score)
	}

	for _, hit := range semanticResults.Hits {
		if len(roomFilter) > 0 && hit.Fields["room_id"] != roomFilter {
			continue
		}
		if len(userFilter) > 0 && hit.Fields["user_id"] != userFilter {
			continue
		}
		r := convertHit(hit)
		r.Score = hit.Score
		result.Semantic = append(result.Semantic, r)
	}

	log.Printf("INFO search: semantic results=%d (limit=%d)", len(result.Semantic), e.cfg.ResultLimit)

	// Limit results
	limit := e.cfg.ResultLimit
	if limit <= 0 {
		limit = 5
	}

	if len(result.Exact) > limit {
		result.Exact = result.Exact[:limit]
	}
	if len(result.Semantic) > limit {
		result.Semantic = result.Semantic[:limit]
	}

	// Deduplicate semantic results against exact results
	seen := make(map[string]bool)
	for _, r := range result.Exact {
		seen[r.EventID] = true
	}

	uniqueSemantic := result.Semantic[:0]
	for _, r := range result.Semantic {
		if !seen[r.EventID] {
			uniqueSemantic = append(uniqueSemantic, r)
		}
	}
	result.Semantic = uniqueSemantic

	return result, nil
}

// SemanticSearch performs only semantic (vector) search without exact text search.
func (e *Engine) SemanticSearch(queryText string, roomFilter, userFilter string) (*SearchResult, error) {
	result := &SearchResult{Query: queryText}

	// Filter stop words from query
	filteredQuery := filterStopWords(queryText)
	if filteredQuery == "" {
		return result, nil
	}

	// Get embedding for semantic search
	vector, err := e.embedClient.CreateEmbedding(filteredQuery)
	if err != nil {
		return nil, fmt.Errorf("create query embedding: %w", err)
	}
	result.QueryVector = vector
	log.Printf("INFO search: semantic query=%q filtered=%q embedding dims=%d first3=%v", queryText, filteredQuery, len(vector), vector[:min(3, len(vector))])

	// Semantic search using Bleve native kNN with FAISS backend
	semanticResults, err := e.bleveClient.SearchSemantic(vector, roomFilter)
	if err != nil {
		return nil, fmt.Errorf("semantic search: %w", err)
	}

	log.Printf("INFO search: semantic total=%d (limit=%d)", len(semanticResults.Hits), e.cfg.ResultLimit)
	for i, hit := range semanticResults.Hits {
		log.Printf("INFO search:   semantic hit[%d] id=%s score=%.6f", i, hit.ID, hit.Score)
	}

	for _, hit := range semanticResults.Hits {
		if len(roomFilter) > 0 && hit.Fields["room_id"] != roomFilter {
			continue
		}
		if len(userFilter) > 0 && hit.Fields["user_id"] != userFilter {
			continue
		}
		r := convertHit(hit)
		r.Score = hit.Score
		result.Semantic = append(result.Semantic, r)
	}

	// Limit results
	limit := e.cfg.ResultLimit
	if limit <= 0 {
		limit = 5
	}
	if len(result.Semantic) > limit {
		result.Semantic = result.Semantic[:limit]
	}

	return result, nil
}

// ExactSearch performs only exact (text) search without semantic search.
func (e *Engine) ExactSearch(queryText string, roomFilter, userFilter string) (*SearchResult, error) {
	result := &SearchResult{Query: queryText}

	// Filter stop words from query
	filteredQuery := filterStopWords(queryText)
	if filteredQuery == "" {
		return result, nil
	}

	// Exact search
	exactResults, err := e.bleveClient.SearchExact(filteredQuery, roomFilter)
	if err != nil {
		return nil, fmt.Errorf("exact search: %w", err)
	}

	for _, hit := range exactResults.Hits {
		if len(userFilter) > 0 && hit.Fields["user_id"] != userFilter {
			continue
		}
		r := convertHit(hit)
		r.Score = hit.Score
		result.Exact = append(result.Exact, r)
	}

	// Limit results
	limit := e.cfg.ResultLimit
	if limit <= 0 {
		limit = 5
	}
	if len(result.Exact) > limit {
		result.Exact = result.Exact[:limit]
	}

	return result, nil
}

func (e *Engine) FormatResults(result *SearchResult) (text, html string) {
	var textParts []string
	var htmlParts []string

	textParts = append(textParts, fmt.Sprintf("*Search results for:* %q", result.Query))
	htmlParts = append(htmlParts, fmt.Sprintf("<b>Search results for:</b> %q", result.Query))

	if len(result.Exact) > 0 {
		textParts = append(textParts, "\n*Exact matches:*")
		htmlParts = append(htmlParts, "<br><b>Exact matches:</b><br/>")
		for i, r := range result.Exact {
			t, h := e.formatResult(r, i+1)
			textParts = append(textParts, t)
			htmlParts = append(htmlParts, h)
		}
	}

	if len(result.Semantic) > 0 {
		textParts = append(textParts, "\n*Similar (semantic):*")
		htmlParts = append(htmlParts, "<br><b>Similar (semantic):</b><br/>")
		for i, r := range result.Semantic {
			t, h := e.formatResult(r, i+1)
			textParts = append(textParts, t)
			htmlParts = append(htmlParts, h)
		}
	}

	if len(result.Exact) == 0 && len(result.Semantic) == 0 {
		textParts = append(textParts, "No results found.")
		htmlParts = append(htmlParts, "No results found.")
	}

	return strings.Join(textParts, "\n"), strings.Join(htmlParts, "\n")
}

func (e *Engine) FormatSemanticResults(result *SearchResult) (text, html string) {
	var textParts []string
	var htmlParts []string

	textParts = append(textParts, fmt.Sprintf("*Semantic search results for:* %q", result.Query))
	htmlParts = append(htmlParts, fmt.Sprintf("<b>Semantic search results for:</b> %q", result.Query))

	if len(result.Semantic) > 0 {
		textParts = append(textParts, "\n*Similar (semantic):*")
		htmlParts = append(htmlParts, "<br><b>Similar (semantic):</b><br/>")
		for i, r := range result.Semantic {
			t, h := e.formatResult(r, i+1)
			textParts = append(textParts, t)
			htmlParts = append(htmlParts, h)
		}
	}

	if len(result.Semantic) == 0 {
		textParts = append(textParts, "No semantic results found.")
		htmlParts = append(htmlParts, "No semantic results found.")
	}

	return strings.Join(textParts, "\n"), strings.Join(htmlParts, "\n")
}

func (e *Engine) formatResult(r Result, idx int) (string, string) {
	var textLine strings.Builder
	var htmlLine strings.Builder

	// Build Matrix-to link for event (Element auto-converts plain URL to pill)
	// Format: https://matrix.to/#/{roomId}/{eventId}
	eventLink := fmt.Sprintf("https://matrix.to/#/%s/%s", r.RoomID, r.EventID)
	fmt.Fprintf(&textLine, "%d. %s", idx, formatTimestamp(r.Timestamp))
	fmt.Fprintf(&htmlLine, "%d. %s", idx, formatTimestamp(r.Timestamp))
	fmt.Fprintf(&htmlLine, " %s", eventLink)
	if r.Score > 0 {
		fmt.Fprintf(&textLine, " (score: %.4f)", r.Score)
		fmt.Fprintf(&htmlLine, " <i>score: %.4f</i>", r.Score)
	}

	// Escape HTML and convert newlines to <br>
	escapeHTML := func(s string) string {
		s = strings.ReplaceAll(s, "&", "&amp;")
		s = strings.ReplaceAll(s, "<", "&lt;")
		s = strings.ReplaceAll(s, ">", "&gt;")
		s = strings.ReplaceAll(s, "\n", "<br>")
		return s
	}

	if r.Text != "" {
		truncated := truncate(r.Text, 200)
		fmt.Fprintf(&textLine, "\n%s\n", truncated)
		fmt.Fprintf(&htmlLine, "<br>%s<br>", escapeHTML(truncated))
	}
	if r.ImageDesc != "" {
		truncated := truncate(r.ImageDesc, 200)
		fmt.Fprintf(&textLine, "\n\U0001f50d %s", truncated)
		fmt.Fprintf(&htmlLine, "<br>\U0001f50d %s", escapeHTML(truncated))
	}

	fmt.Fprint(&textLine, "\n")
	fmt.Fprint(&htmlLine, "<br>")

	return textLine.String(), htmlLine.String()
}

func formatTimestamp(ts int64) string {
	return time.UnixMilli(ts).Format("2006-01-02 15:04:05")
}

func truncate(s string, maxLen int) string {
	if utf8.RuneCountInString(s) <= maxLen {
		return s
	}
	runes := []rune(s)
	return string(runes[:maxLen]) + "..."
}

func convertHit(hit *search.DocumentMatch) Result {
	var r Result
	r.Score = hit.Score
	if v, ok := hit.Fields["event_id"]; ok {
		if s, ok := v.(string); ok {
			r.EventID = s
		}
	}
	if v, ok := hit.Fields["room_id"]; ok {
		if s, ok := v.(string); ok {
			r.RoomID = s
		}
	}
	if v, ok := hit.Fields["user_id"]; ok {
		if s, ok := v.(string); ok {
			r.UserID = s
		}
	}
	if v, ok := hit.Fields["text"]; ok {
		if s, ok := v.(string); ok {
			r.Text = s
		}
	}
	if v, ok := hit.Fields["image_desc"]; ok {
		if s, ok := v.(string); ok {
			r.ImageDesc = s
		}
	}
	if v, ok := hit.Fields["raw_url"]; ok {
		if s, ok := v.(string); ok {
			r.RawURL = s
		}
	}
	if v, ok := hit.Fields["file_name"]; ok {
		if s, ok := v.(string); ok {
			r.FileName = s
		}
	}
	if v, ok := hit.Fields["timestamp"]; ok {
		switch tv := v.(type) {
		case float64:
			r.Timestamp = int64(tv)
		case string:
			if ts, err := fmt.Sscanf(tv, "%d", &r.Timestamp); err == nil && ts == 1 {
				// parsed
			}
		}
	}
	return r
}
