package bot

import (
	"fmt"
	"slices"
	"strings"

	"github.com/rkfg/guiltyspark/indexer"
	"github.com/rkfg/guiltyspark/search"
)

var commandAliases = map[string][]string{
	"search":          {"s", "find", "искать", "поиск", "п"},
	"search-semantic": {"semantic", "семантик", "сем", "с", "similarity", "similar", "related"},
	"help":            {"?", "помощь", "h"},
	"stats":           {"status", "стат", "info"},
}

func HelpText() string {
	commands := []struct {
		name        string
		description string
	}{
		{"search", "Exact text search"},
		{"search-semantic", "Semantic (vector) similarity search"},
		{"stats", "Show index statistics"},
		{"help", "Show this help message"},
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
			fmt.Fprintf(&sb, "!%s — %s (aliases: %s)\n", cmd.name, cmd.description, strings.Join(aliasStrs, ", "))
		} else {
			fmt.Fprintf(&sb, "!%s — %s\n", cmd.name, cmd.description)
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
}

func ParseCommandArgs(args string) (*CommandArgs, error) {
	ca := &CommandArgs{}

	parts := strings.Split(args, " ")

	for i := 0; i < len(parts); i++ {
		part := parts[i]
		switch part {
		case "--user":
			if i+1 < len(parts) {
				ca.UserFilter = parts[i+1]
				i++
			}
		default:
			if ca.Query == "" {
				ca.Query = part
			} else {
				ca.Query += " " + part
			}
		}
	}

	if ca.Query == "" {
		return nil, fmt.Errorf("no query provided")
	}

	return ca, nil
}

func (h *CommandHandler) HandleSearch(args string, roomID string, userFilter string) (string, string, error) {
	ca, err := ParseCommandArgs(args)
	if err != nil {
		return "Usage: !search <query> #room:server.org", "", nil
	}

	// Room filter is REQUIRED for search
	if roomID == "" {
		return "Error: You must specify a room to search. Use a room alias like #room:server.org", "", nil
	}
	
	// Merge user filter from HTML with --user arg
	if userFilter != "" && ca.UserFilter == "" {
		ca.UserFilter = userFilter
	}

	result, err := h.searchEngine.ExactSearch(ca.Query, roomID, ca.UserFilter)
	if err != nil {
		return fmt.Sprintf("Search error: %v", err), "", err
	}

	textResult, htmlResult := h.searchEngine.FormatResults(result)
	return textResult, htmlResult, nil
}

func (h *CommandHandler) HandleSemanticSearch(args string, roomID string, userFilter string) (string, string, error) {
	ca, err := ParseCommandArgs(args)
	if err != nil {
		return "Usage: !semantic <query> #room:server.org", "", nil
	}

	// Room filter is REQUIRED for semantic search
	if roomID == "" {
		return "Error: You must specify a room to search. Use a room alias like #room:server.org", "", nil
	}
	
	// Merge user filter from HTML with --user arg
	if userFilter != "" && ca.UserFilter == "" {
		ca.UserFilter = userFilter
	}

	result, err := h.searchEngine.SemanticSearch(ca.Query, roomID, ca.UserFilter)
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
