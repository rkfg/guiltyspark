package bot

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"maunium.net/go/mautrix/event"
)

func TestExtractURLs_table(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		maxURLs int
		want    []string
	}{
		{"simple URL", "check out https://example.com for more", 5, []string{"https://example.com"}},
		{"multiple URLs", "https://a.com and https://b.com", 5, []string{"https://a.com", "https://b.com"}},
		{"duplicates", "https://example.com https://example.com", 5, []string{"https://example.com", "https://example.com"}},
		{"maxURLs=2", "https://a.com https://b.com https://c.com", 2, []string{"https://a.com", "https://b.com"}},
		{"trailing punctuation", "check https://example.com. and https://example.com!", 5, []string{"https://example.com", "https://example.com"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractURLs(tt.input, tt.maxURLs)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestExtractURLs_skip_matrix_to(t *testing.T) {
	input := "https://matrix.to/#/room1 https://example.com https://matrix.to/#/@user"
	got := extractURLs(input, 5)
	assert.Equal(t, []string{"https://example.com"}, got)
}

func TestShouldSkipPreview(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"https://matrix.to/#/room1", true},
		{"https://matrix.to/#/@user", true},
		{"https://example.com", false},
		{"https://google.com", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := shouldSkipPreview(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBuildTextWithPreviews_no_previews(t *testing.T) {
	input := "hello world"
	got := buildTextWithPreviews(input, nil)
	assert.Equal(t, input, got)
}

func TestBuildTextWithPreviews_with_previews(t *testing.T) {
	input := "check this out"
	previews := []*event.LinkPreview{
		{Title: "Example", Description: "A test page"},
		{Title: "Another", Description: ""},
	}
	got := buildTextWithPreviews(input, previews)
	assert.Contains(t, got, input)
	assert.Contains(t, got, "preview: [Example]")
	assert.Contains(t, got, " - A test page")
	assert.Contains(t, got, "preview: [Another]")
}
