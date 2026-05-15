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

const searchQueryPrefix = "search_query: "

type EmbedClient interface {
	CreateEmbedding(text, prefix string) ([]float32, error)
}
type Engine struct {
	bleveClient *indexer.BleveClient
	embedClient EmbedClient
	cfg         *config.SearchConfig
}

func NewEngine(bleveClient *indexer.BleveClient, embedClient EmbedClient, cfg *config.SearchConfig) *Engine {
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

const (
	punctuationTrim  = ".,!?;:\"'()[]{}"
	textTruncateLen  = 200
	imageTruncateLen = 200
	defaultLimit     = 5

	noResultsText  = "No results found."
	noSemanticText = "No semantic results found."
)

func filterStopWords(query string) string {
	words := strings.Fields(strings.ToLower(query))
	var filtered []string
	for _, w := range words {
		clean := strings.Trim(w, punctuationTrim)
		if clean != "" && !russianStopWords[clean] {
			filtered = append(filtered, clean)
		}
	}
	return strings.Join(filtered, " ")
}

func limitResults(results []Result, limit int) []Result {
	if limit <= 0 {
		limit = defaultLimit
	}
	if len(results) > limit {
		return results[:limit]
	}
	return results
}

func filterHitsByUserAndRoom(hits []*search.DocumentMatch, roomFilter, userFilter string) []Result {
	var results []Result
	for _, hit := range hits {
		if len(roomFilter) > 0 && hit.Fields["room_id"] != roomFilter {
			continue
		}
		if len(userFilter) > 0 && hit.Fields["user_id"] != userFilter {
			continue
		}
		r := convertHit(hit)
		r.Score = hit.Score
		results = append(results, r)
	}
	return results
}

func escapeHTML(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, "\n", "<br>")
	return s
}

func (e *Engine) prepareQuery(queryText string) (string, []float32) {
	filteredQuery := filterStopWords(queryText)
	if filteredQuery == "" {
		return "", nil
	}
	vector, err := e.embedClient.CreateEmbedding(filteredQuery, searchQueryPrefix)
	if err != nil {
		panic(fmt.Errorf("create query embedding: %w", err))
	}
	log.Printf("INFO search: query=%q filtered=%q embedding dims=%d first3=%v", queryText, filteredQuery, len(vector), vector[:min(3, len(vector))])
	return filteredQuery, vector
}

func appendFormattedResults(textParts, htmlParts []string, results []Result, sectionHeaderText, sectionHeaderHTML string) ([]string, []string) {
	if len(results) == 0 {
		return textParts, htmlParts
	}
	textParts = append(textParts, "\n"+sectionHeaderText)
	htmlParts = append(htmlParts, "<br>"+sectionHeaderHTML+"<br/>")
	for i, r := range results {
		t, h := formatResult(r, i+1)
		textParts = append(textParts, t)
		htmlParts = append(htmlParts, h)
	}
	return textParts, htmlParts
}

func formatResult(r Result, idx int) (string, string) {
	var textLine strings.Builder
	var htmlLine strings.Builder

	eventLink := fmt.Sprintf("https://matrix.to/#/%s/%s", r.RoomID, r.EventID)
	ts := formatTimestamp(r.Timestamp)
	fmt.Fprintf(&textLine, "%d. %s", idx, ts)
	fmt.Fprintf(&htmlLine, "%d. %s %s", idx, ts, eventLink)
	if r.UserID != "" {
		textLabel, htmlLink := formatAuthor(r.UserID)
		fmt.Fprintf(&textLine, " by %s", textLabel)
		fmt.Fprintf(&htmlLine, " by %s", htmlLink)
	}
	if r.Score > 0 {
		fmt.Fprintf(&textLine, " (score: %.4f)", r.Score)
		fmt.Fprintf(&htmlLine, " <i>score: %.4f</i>", r.Score)
	}

	if r.Text != "" {
		truncated := truncate(r.Text, textTruncateLen)
		fmt.Fprintf(&textLine, "\n%s\n", truncated)
		fmt.Fprintf(&htmlLine, "<br>%s<br>", escapeHTML(truncated))
	}
	if r.ImageDesc != "" {
		truncated := truncate(r.ImageDesc, imageTruncateLen)
		fmt.Fprintf(&textLine, "\n\U0001f50d %s", truncated)
		fmt.Fprintf(&htmlLine, "<br>\U0001f50d %s", escapeHTML(truncated))
	}

	fmt.Fprint(&textLine, "\n")
	fmt.Fprint(&htmlLine, "<br>")

	return textLine.String(), htmlLine.String()
}

// SemanticSearch performs only semantic (vector) search without exact text search.
func (e *Engine) SemanticSearch(queryText string, roomFilter, userFilter string) (*SearchResult, error) {
	filteredQuery, vector := e.prepareQuery(queryText)
	if filteredQuery == "" {
		return &SearchResult{Query: queryText}, nil
	}

	result := &SearchResult{Query: queryText}
	result.QueryVector = vector

	// Semantic search using Bleve native kNN with FAISS backend
	semanticResults, err := e.bleveClient.SearchSemantic(vector, roomFilter)
	if err != nil {
		return nil, fmt.Errorf("semantic search: %w", err)
	}

	result.Semantic = filterHitsByUserAndRoom(semanticResults.Hits, roomFilter, userFilter)
	result.Semantic = limitResults(result.Semantic, e.cfg.ResultLimit)

	return result, nil
}

// ExactSearch performs only exact (text) search without semantic search.
func (e *Engine) ExactSearch(queryText string, roomFilter, userFilter string) (*SearchResult, error) {
	filteredQuery := filterStopWords(queryText)
	if filteredQuery == "" {
		return &SearchResult{Query: queryText}, nil
	}

	result := &SearchResult{Query: queryText}

	// Exact search
	exactResults, err := e.bleveClient.SearchExact(filteredQuery, roomFilter)
	if err != nil {
		return nil, fmt.Errorf("exact search: %w", err)
	}

	result.Exact = filterHitsByUserAndRoom(exactResults.Hits, "", userFilter)
	result.Exact = limitResults(result.Exact, e.cfg.ResultLimit)

	return result, nil
}

func (e *Engine) FormatResults(result *SearchResult) (text, html string) {
	var textParts []string
	var htmlParts []string

	textParts = append(textParts, fmt.Sprintf("*Search results for:* %q", result.Query))
	htmlParts = append(htmlParts, fmt.Sprintf("<b>Search results for:</b> %q", result.Query))

	textParts, htmlParts = appendFormattedResults(textParts, htmlParts, result.Exact, "*Exact matches:*", "<b>Exact matches:</b>")
	textParts, htmlParts = appendFormattedResults(textParts, htmlParts, result.Semantic, "*Similar (semantic):*", "<b>Similar (semantic):</b>")

	if len(result.Exact) == 0 && len(result.Semantic) == 0 {
		textParts = append(textParts, noResultsText)
		htmlParts = append(htmlParts, noResultsText)
	}

	return strings.Join(textParts, "\n"), strings.Join(htmlParts, "\n")
}

func (e *Engine) FormatSemanticResults(result *SearchResult) (text, html string) {
	var textParts []string
	var htmlParts []string

	textParts = append(textParts, fmt.Sprintf("*Semantic search results for:* %q", result.Query))
	htmlParts = append(htmlParts, fmt.Sprintf("<b>Semantic search results for:</b> %q", result.Query))

	textParts, htmlParts = appendFormattedResults(textParts, htmlParts, result.Semantic, "*Similar (semantic):*", "<b>Similar (semantic):</b>")

	if len(result.Semantic) == 0 {
		textParts = append(textParts, noSemanticText)
		htmlParts = append(htmlParts, noSemanticText)
	}

	return strings.Join(textParts, "\n"), strings.Join(htmlParts, "\n")
}

func formatTimestamp(ts int64) string {
	return time.UnixMilli(ts).Format("2006-01-02 15:04:05")
}

func formatAuthor(userID string) (textLabel, htmlLink string) {
	if userID == "" {
		return "", ""
	}
	parts := strings.SplitN(userID, ":", 2)
	if len(parts) == 2 {
		localpart := parts[0]
		return localpart, fmt.Sprintf(`<a href="https://matrix.to/#/%s">%s</a>`, userID, localpart)
	}
	return userID, fmt.Sprintf(`<a href="https://matrix.to/#/%s">%s</a>`, userID, userID)
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
