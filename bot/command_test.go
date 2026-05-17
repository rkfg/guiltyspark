package bot

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResolveCommand_table(t *testing.T) {
	tests := []struct {
		input     string
		canonical string
		args      string
		isCmd     bool
	}{
		// search canonical
		{"!search hello", "search", "hello", true},
		{"!search-semantic query", "search-semantic", "query", true},
		{"search hello", "search", "hello", true},
		{"search-semantic query", "search-semantic", "query", true},
		// search aliases
		{"!s hello", "search", "hello", true},
		{"s hello", "search", "hello", true},
		{"!ы hello", "search", "hello", true},
		{"ы hello", "search", "hello", true},
		{"!find hello", "search", "hello", true},
		{"find hello", "search", "hello", true},
		{"!искать hello", "search", "hello", true},
		{"искать hello", "search", "hello", true},
		{"!поиск hello", "search", "hello", true},
		{"поиск hello", "search", "hello", true},
		{"!п hello", "search", "hello", true},
		{"п hello", "search", "hello", true},
		{"!g hello", "search", "hello", true},
		{"g hello", "search", "hello", true},
		// search-semantic aliases
		{"!semantic query", "search-semantic", "query", true},
		{"semantic query", "search-semantic", "query", true},
		{"!семантик query", "search-semantic", "query", true},
		{"семантик query", "search-semantic", "query", true},
		{"!сем query", "search-semantic", "query", true},
		{"сем query", "search-semantic", "query", true},
		{"!с query", "search-semantic", "query", true},
		{"с query", "search-semantic", "query", true},
		{"!c query", "search-semantic", "query", true},
		{"c query", "search-semantic", "query", true},
		{"!similarity query", "search-semantic", "query", true},
		{"similarity query", "search-semantic", "query", true},
		{"!sim query", "search-semantic", "query", true},
		{"sim query", "search-semantic", "query", true},
		{"!similar query", "search-semantic", "query", true},
		{"similar query", "search-semantic", "query", true},
		{"!related query", "search-semantic", "query", true},
		{"related query", "search-semantic", "query", true},
		// help canonical
		{"!help", "help", "", true},
		{"help", "help", "", true},
		// help aliases
		{"!?", "help", "", true},
		{"?", "help", "", true},
		{"!помощь", "help", "", true},
		{"помощь", "help", "", true},
		{"!h", "help", "", true},
		{"h", "help", "", true},
		{"!р", "help", "", true},
		{"р", "help", "", true},
		// stats canonical
		{"!stats", "stats", "", true},
		{"stats", "stats", "", true},
		// stats aliases
		{"!status", "stats", "", true},
		{"status", "stats", "", true},
		{"!стат", "stats", "", true},
		{"стат", "stats", "", true},
		{"!info", "stats", "", true},
		{"info", "stats", "", true},
		// unknown command
		{"!unknown", "", "", false},
		{"unknown", "", "", false},
		// case-insensitive
		{"!Search hello", "search", "hello", true},
		{"!S hello", "search", "hello", true},
		{"!SEARCH hello", "search", "hello", true},
		{"!Help", "help", "", true},
		{"!HELP", "help", "", true},
		// commands with args
		{"!search hello world", "search", "hello world", true},
		{"!search --user @bob:matrix.org", "search", "--user @bob:matrix.org", true},
		{"!stats extra", "stats", "extra", true},
		// empty
		{"!s", "search", "", true},
		{"!search", "search", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			canonical, args, isCmd := ResolveCommand(tt.input)
			assert.Equal(t, tt.canonical, canonical)
			assert.Equal(t, tt.args, args)
			assert.Equal(t, tt.isCmd, isCmd)
		})
	}
}

func TestParseCommandArgs_table(t *testing.T) {
	tests := []struct {
		name     string
		args     string
		query    string
		user     string
		hasError bool
	}{
		{"simple query", "hello", "hello", "", false},
		{"query with spaces", "hello world", "hello world", "", false},
		{"with --user", "hello --user @bob:matrix.org", "hello", "@bob:matrix.org", false},
		{"--user without query", "--user @bob:matrix.org", "", "", true},
		{"query after --user", "--user @bob:matrix.org hello", "hello", "@bob:matrix.org", false},
		{"query with spaces after --user", "--user @bob:matrix.org hello world", "hello world", "@bob:matrix.org", false},
		{"empty query", "", "", "", true},
		{"query with extra spaces", "  hello  world  ", "hello world", "", false},
		{"multiple spaces between args", "hello  --user  @bob:matrix.org  world", "hello @bob:matrix.org world", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ca, err := ParseCommandArgs(tt.args)
			if tt.hasError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.query, ca.Query)
			assert.Equal(t, tt.user, ca.UserFilter)
		})
	}
}

func TestStripCommandAlias_table(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		command string
		want    string
	}{
		{"search with !", "!search hello", "search", "hello"},
		{"search without !", "search hello", "search", "hello"},
		{"s alias with !", "!s hello", "search", "hello"},
		{"s alias without !", "s hello", "search", "hello"},
		{"help without space", "!help", "help", "!help"},
		{"empty input", "", "search", ""},
		{"empty command", "hello", "", "hello"},
		{"unknown command strips prefix", "!unknown hello", "unknown", "hello"},
		{"search with args", "!search hello world", "search", "hello world"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stripCommandAlias(tt.input, tt.command)
			assert.Equal(t, tt.want, got)
		})
	}
}


