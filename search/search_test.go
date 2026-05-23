package search

import (
	"testing"
	"time"
	"unicode/utf8"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/rkfg/guiltyspark/config"
	"github.com/stretchr/testify/assert"
)

var localLoc, _ = time.LoadLocation("Local")

func TestFilterStopWords_table(t *testing.T) {
	tests := []struct {
		name string
		query string
		want string
	}{
		{"no stop words", "hello world", "hello world"},
		{"all stop words", "the and or", ""},
		{"with punctuation", "hello & world", "hello & world"},
		{"russian stop words only", "и это в том", "том"},
		{"mixed stop words", "я иду в магазин", "иду магазин"},
		{"empty query", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filterStopWords(tt.query)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLimitResults_table(t *testing.T) {
	results := make([]Result, 3)
	results[0] = Result{Text: "a"}
	results[1] = Result{Text: "b"}
	results[2] = Result{Text: "c"}

	tests := []struct {
		name   string
		results []Result
		limit  int
		want   int
	}{
		{"limit 3, 3 results", results, 3, 3},
		{"limit 5, 3 results", results, 5, 3},
		{"limit 1, 3 results", results, 1, 1},
		{"limit 0, 3 results (default=5)", results, 0, 3},
		{"0 results", make([]Result, 0), 5, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := limitResults(tt.results, tt.limit)
			assert.Equal(t, tt.want, len(got))
		})
	}
}

func TestFormatResult(t *testing.T) {
	ts := time.Date(2021, 1, 1, 0, 0, 0, 0, localLoc)
	tsUnix := ts.UnixMilli()

	tests := []struct {
		name   string
		result Result
		idx    int
		hasTextPrefix string
		hasHTMLPrefix string
	}{
		{
			name:   "full result",
			result: Result{EventID: "evt1", RoomID: "#room:server.org", UserID: "@bob:matrix.org", Timestamp: tsUnix, Text: "hello world", Score: 0.95, ImageDesc: "a cat"},
			idx:    1,
			hasTextPrefix: "1. 2021-01-01 00:00:00 by @bob (score: 0.9500)\nhello world",
			hasHTMLPrefix: "1. 2021-01-01 00:00:00 https://matrix.to/#/#room:server.org/evt1 by <a href=\"https://matrix.to/#/@bob:matrix.org\">@bob</a> <i>score: 0.9500</i><br>hello world",
		},
		{
			name:   "text only",
			result: Result{EventID: "evt1", RoomID: "#room:server.org", Timestamp: ts.UnixMilli(), Text: "hello world", Score: 0.5},
			idx:    1,
			hasTextPrefix: "1. 2021-01-01 00:00:00 (score: 0.5000)\nhello world",
			hasHTMLPrefix: "1. 2021-01-01 00:00:00 https://matrix.to/#/#room:server.org/evt1 <i>score: 0.5000</i><br>hello world",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			text, html := formatResult(tt.result, tt.idx)
			assert.Contains(t, text, tt.hasTextPrefix)
			assert.Contains(t, html, tt.hasHTMLPrefix)
		})
	}
}

func TestFormatResults_no_results(t *testing.T) {
	eng := &Engine{cfg: &config.SearchConfig{ResultLimit: 5}}
	text, html := eng.FormatResults(&SearchResult{Query: "test"})
	assert.Contains(t, text, "No results found.")
	assert.Contains(t, html, "No results found.")
}

func TestFormatResults_with_results(t *testing.T) {
	eng := &Engine{cfg: &config.SearchConfig{ResultLimit: 5}}
	ts := time.Date(2021, 1, 1, 0, 0, 0, 0, localLoc).UnixMilli()
	result := &SearchResult{
		Query: "test",
		Exact: []Result{
			{EventID: "evt1", RoomID: "#room:server.org", UserID: "@bob:matrix.org", Timestamp: ts, Text: "hello world", Score: 0.95},
		},
		Semantic: []Result{
			{EventID: "evt2", RoomID: "#room:server.org", UserID: "@bob:matrix.org", Timestamp: ts, Text: "goodbye world", Score: 0.85},
		},
	}

	text, html := eng.FormatResults(result)
	assert.Contains(t, text, "Search results for:")
	assert.Contains(t, text, "Exact matches:")
	assert.Contains(t, text, "Similar (semantic):")
	assert.Contains(t, text, "hello world")
	assert.Contains(t, text, "goodbye world")
	assert.Contains(t, html, "Search results for:")
	assert.Contains(t, html, "Exact matches:")
	assert.Contains(t, html, "Similar (semantic):")
}

func TestFilterStopWords_date_filters_not_in_query(t *testing.T) {
	// Date filters are not stop words, they should pass through
	got := filterStopWords("before:2026-05-02 after:2026-02-03")
	assert.Equal(t, "before:2026-05-02 after:2026-02-03", got)
}

func TestFilterByDate_range(t *testing.T) {
	// Timestamps in UTC (Matrix stores timestamps in UTC)
	ts1 := time.Date(2021, 6, 1, 12, 0, 0, 0, time.UTC)  // May 20, 2021 12:00 UTC
	ts2 := time.Date(2023, 1, 15, 12, 0, 0, 0, time.UTC) // May 19, 2023 12:00 UTC

	before20220601 := time.Date(2022, 6, 1, 0, 0, 0, 0, localLoc)  // before June 1 00:00 local
	after20200501 := time.Date(2020, 5, 1, 0, 0, 0, 0, localLoc)   // after May 1 00:00 local

	hits := []*search.DocumentMatch{
		{
			Fields: map[string]any{
				"event_id":  "evt1",
				"room_id":   "room1",
				"user_id":   "@alice:example.org",
				"timestamp": float64(ts1.UnixMilli()),
			},
		},
		{
			Fields: map[string]any{
				"event_id":  "evt2",
				"room_id":   "room1",
				"user_id":   "@bob:example.org",
				"timestamp": float64(ts2.UnixMilli()),
			},
		},
	}

	// before 2022-06-01 local — ts1 (2021-06-01) should be included
	result := filterHitsByUserAndRoom(hits, SearchArgs{BeforeDate: &before20220601})
	assert.Equal(t, 1, len(result))
	assert.Equal(t, "evt1", result[0].EventID)

	// after 2020-05-01 local — ts1 (2021-06-01) should be included
	result = filterHitsByUserAndRoom(hits, SearchArgs{AfterDate: &after20200501})
	assert.Equal(t, 2, len(result))
	assert.Equal(t, "evt1", result[0].EventID)
	assert.Equal(t, "evt2", result[1].EventID)

	// before 2022-06-01 AND after 2020-05-01 local — ts1 (2021-06-01) should be included
	result = filterHitsByUserAndRoom(hits, SearchArgs{BeforeDate: &before20220601, AfterDate: &after20200501})
	assert.Equal(t, 1, len(result))
	assert.Equal(t, "evt1", result[0].EventID)

	// before 2021-01-01 local — ts1 (2021-06-01) should be excluded (after the date)
	before20210101 := time.Date(2021, 1, 1, 0, 0, 0, 0, localLoc)
	result = filterHitsByUserAndRoom(hits, SearchArgs{BeforeDate: &before20210101})
	assert.Equal(t, 0, len(result))
}

func TestFormatSemanticResults_no_results(t *testing.T) {
	eng := &Engine{cfg: &config.SearchConfig{ResultLimit: 5}}
	text, html := eng.FormatSemanticResults(&SearchResult{Query: "test"})
	assert.Contains(t, text, "No semantic results found.")
	assert.Contains(t, html, "No semantic results found.")
}

func TestFormatSemanticResults_with_results(t *testing.T) {
	eng := &Engine{cfg: &config.SearchConfig{ResultLimit: 5}}
	ts := time.Date(2021, 1, 1, 0, 0, 0, 0, localLoc).UnixMilli()
	result := &SearchResult{
		Query: "test",
		Semantic: []Result{
			{EventID: "evt1", RoomID: "#room:server.org", UserID: "@bob:matrix.org", Timestamp: ts, Text: "hello world", Score: 0.95},
		},
	}

	text, html := eng.FormatSemanticResults(result)
	assert.Contains(t, text, "Semantic search results for:")
	assert.Contains(t, text, "Similar (semantic):")
	assert.Contains(t, text, "hello world")
	assert.Contains(t, html, "Semantic search results for:")
	assert.Contains(t, html, "Similar (semantic):")
}

func TestTruncate_unicode_cyrillic(t *testing.T) {
	input := "привет мир это длинный текст"
	// 5 runes = "приве" + "..." = 8 runes total
	result := truncate(input, 5)
	runes := utf8.RuneCountInString(result)
	assert.Equal(t, 8, runes) // 5 truncated runes + 3 for "..."
	assert.Equal(t, "приве...", result)
}
