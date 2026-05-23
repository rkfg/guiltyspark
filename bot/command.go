package bot

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/rkfg/guiltyspark/indexer"
	"github.com/rkfg/guiltyspark/search"
)

var localLoc, _ = time.LoadLocation("Local")

var commandAliases = map[string][]string{
	"search":          {"s", "ы", "find", "искать", "поиск", "п", "g"},
	"search-semantic": {"semantic", "семантик", "сем", "с", "c", "similarity", "sim", "similar", "related"},
	"help":            {"?", "помощь", "h", "р"},
	"stats":           {"status", "стат", "info"},
}

func HelpText() string {
	commands := []struct {
		name        string
		description string
		format      string
	}{
		{"search", "Exact text search", "search <query> #room:server.org [user:<user_id>] [before:YYYY-MM-DD] [after:YYYY-MM-DD]"},
		{"search-semantic", "Semantic (vector) similarity search", "semantic <query> #room:server.org [user:<user_id>] [before:YYYY-MM-DD] [after:YYYY-MM-DD]"},
		{"stats", "Show index statistics", "stats"},
		{"help", "Show this help message", "help"},
	}

	var sb strings.Builder
	sb.WriteString("**Available commands:**\n")
	for _, cmd := range commands {
		aliases := commandAliases[cmd.name]
		if len(aliases) > 0 {
			aliasStrs := make([]string, len(aliases))
			for i, a := range aliases {
				aliasStrs[i] = "!" + a
			}
			fmt.Fprintf(&sb, "!%s — %s\n", cmd.name, cmd.description)
			fmt.Fprintf(&sb, "  format: %s\n", cmd.format)
			fmt.Fprintf(&sb, "  aliases: %s\n", strings.Join(aliasStrs, ", "))
		} else {
			fmt.Fprintf(&sb, "!%s — %s\n", cmd.name, cmd.description)
			fmt.Fprintf(&sb, "  format: %s\n", cmd.format)
		}
	}
	return sb.String()
}

// ResolveCommand resolves an alias to the canonical command name.
// Returns the canonical name, the arguments (everything after the command), and true if it's a known command.
func ResolveCommand(cmd string) (string, args string, isCommand bool) {
	cmd = strings.ToLower(strings.TrimPrefix(cmd, "!"))
	cmd = strings.TrimSpace(cmd)

	// Split into command and arguments
	parts := strings.SplitN(cmd, " ", 2)
	commandName := parts[0]
	args = ""
	if len(parts) > 1 {
		args = strings.TrimSpace(parts[1])
	}

	// Check if it's a canonical command name (key in the map)
	if _, ok := commandAliases[commandName]; ok {
		return commandName, args, true
	}

	// Check if it's an alias
	for canonical, aliases := range commandAliases {
		if slices.Contains(aliases, commandName) {
			return canonical, args, true
		}
	}

	return "", "", false
}

type CommandHandler struct {
	searchEngine *search.Engine
}

func NewCommandHandler(searchEngine *search.Engine) *CommandHandler {
	return &CommandHandler{searchEngine: searchEngine}
}

type CommandArgs struct {
	Query      string
	RoomFilter string
	UserFilter string
	BeforeDate *time.Time
	AfterDate  *time.Time
}

func (ca *CommandArgs) toSearchArgs() search.SearchArgs {
	return search.SearchArgs{
		RoomID:     ca.RoomFilter,
		UserFilter: ca.UserFilter,
		BeforeDate: ca.BeforeDate,
		AfterDate:  ca.AfterDate,
	}
}

var (
	userRegex    = regexp.MustCompile(`user:\s*(@\S+)`)
	beforeRegex  = regexp.MustCompile(`before:\s*(\d{4}-\d{2}-\d{2})`)
	afterRegex   = regexp.MustCompile(`after:\s*(\d{4}-\d{2}-\d{2})`)
)

func ParseCommandArgs(args string) (*CommandArgs, error) {
	ca := &CommandArgs{}

	// Extract date filters from the query
	if matches := beforeRegex.FindStringSubmatch(args); len(matches) > 1 {
		if d, err := time.ParseInLocation("2006-01-02", matches[1], localLoc); err == nil {
			ca.BeforeDate = &d
		}
	}
	if matches := afterRegex.FindStringSubmatch(args); len(matches) > 1 {
		if d, err := time.ParseInLocation("2006-01-02", matches[1], localLoc); err == nil {
			ca.AfterDate = &d
		}
	}

	// Extract user filter from the query
	if matches := userRegex.FindStringSubmatch(args); len(matches) > 1 {
		ca.UserFilter = strings.TrimSpace(matches[1])
	}

	// Remove all filter patterns from the query to get the raw query text
	queryText := beforeRegex.ReplaceAllString(args, "")
	queryText = afterRegex.ReplaceAllString(queryText, "")
	queryText = userRegex.ReplaceAllString(queryText, "")
	// Remove room links (they don't affect the query text)
	queryText = regexp.MustCompile(`[#!]?\S*:\S+`).ReplaceAllString(queryText, "")
	// Normalize whitespace
	queryText = regexp.MustCompile(`\s+`).ReplaceAllString(queryText, " ")

	ca.Query = strings.TrimSpace(queryText)

	if ca.Query == "" {
		return nil, fmt.Errorf("no query provided")
	}

	return ca, nil
}

func (h *CommandHandler) HandleSearch(args string, roomID string, userFilter string, lastUsedRoom string) (string, string, error) {
	ca, err := ParseCommandArgs(args)
	if err != nil {
		return "Usage: !search <query> #room:server.org [user:<user_id>] [before:YYYY-MM-DD] [after:YYYY-MM-DD]", "", nil
	}

	// Room filter is REQUIRED for search
	if roomID == "" {
		roomID = lastUsedRoom
	}
	if roomID == "" {
		return "Error: You must specify a room to search. Use a room alias like #room:server.org", "", nil
	}

	// Merge user filter from HTML with user: arg
	if userFilter != "" && ca.UserFilter == "" {
		ca.UserFilter = userFilter
	}
	ca.RoomFilter = roomID

	searchArgs := ca.toSearchArgs()
	result, err := h.searchEngine.ExactSearch(ca.Query, searchArgs)
	if err != nil {
		return fmt.Sprintf("Search error: %v", err), "", err
	}

	textResult, htmlResult := h.searchEngine.FormatResults(result)
	return textResult, htmlResult, nil
}

func (h *CommandHandler) HandleSemanticSearch(args string, roomID string, userFilter string, lastUsedRoom string) (string, string, error) {
	ca, err := ParseCommandArgs(args)
	if err != nil {
		return "Usage: !semantic <query> #room:server.org [user:<user_id>] [before:YYYY-MM-DD] [after:YYYY-MM-DD]", "", nil
	}

	// Room filter is REQUIRED for semantic search
	if roomID == "" {
		roomID = lastUsedRoom
	}
	if roomID == "" {
		return "Error: You must specify a room to search. Use a room alias like #room:server.org", "", nil
	}

	// Merge user filter from HTML with user: arg
	if userFilter != "" && ca.UserFilter == "" {
		ca.UserFilter = userFilter
	}
	ca.RoomFilter = roomID

	searchArgs := ca.toSearchArgs()
	result, err := h.searchEngine.SemanticSearch(ca.Query, searchArgs)
	if err != nil {
		return fmt.Sprintf("Semantic search error: %v", err), "", err
	}

	textResult, htmlResult := h.searchEngine.FormatSemanticResults(result)
	return textResult, htmlResult, nil
}

func (h *CommandHandler) HandleStats(bleveClient *indexer.BleveClient) (string, error) {
	if bleveClient == nil {
		return "Error: Bleve client not available", nil
	}

	total, err := bleveClient.CountDocuments()
	if err != nil {
		return fmt.Sprintf("Error counting documents: %v", err), nil
	}

	return fmt.Sprintf("Index statistics:\nTotal documents: %d", total), nil
}
