package bot

import (
	"fmt"
	"strings"

	"github.com/rkfg/guiltyspark/indexer"
	"github.com/rkfg/guiltyspark/search"
)

var commandAliases = map[string][]string{
	"search":          {"s", "find", "искать", "поиск"},
	"search-semantic": {"semantic", "семант", "similarity", "similar", "related"},
	"help":            {"?", "помощь", "h"},
	"stats":           {"status", "статистика", "info"},
}

func HelpText() string {
	return `**Available commands:**
!search <query> — Exact text search (aliases: !s, !find, !поиск, !искать)
!semantic <query> — Semantic (vector) similarity search (aliases: !semantic, !семант, !similarity, !similar, !related)
!stats — Show index statistics (aliases: !status, !статистика, !info)
!help — Show this help message (aliases: !?, !помощь, !h)`
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
		for _, alias := range aliases {
			if alias == commandName {
				return canonical, args, true
			}
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
		case "--room":
			if i+1 < len(parts) {
				ca.RoomFilter = parts[i+1]
				i++
			}
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

func (h *CommandHandler) HandleSearch(args string, roomID string) (string, string, error) {
	ca, err := ParseCommandArgs(args)
	if err != nil {
		return "Usage: !search <query> [--room <room_id>] [--user <user_id>]", "", nil
	}

	// Use the current room as the default filter if not explicitly overridden
	if ca.RoomFilter == "" {
		ca.RoomFilter = roomID
	}

	result, err := h.searchEngine.ExactSearch(ca.Query, ca.RoomFilter, ca.UserFilter)
	if err != nil {
		return fmt.Sprintf("Search error: %v", err), "", err
	}

	textResult, htmlResult := h.searchEngine.FormatResults(result)
	return textResult, htmlResult, nil
}

func (h *CommandHandler) HandleSemanticSearch(args string, roomID string) (string, string, error) {
	ca, err := ParseCommandArgs(args)
	if err != nil {
		return "Usage: !semantic <query> [--room <room_id>] [--user <user_id>]", "", nil
	}

	// Use the current room as the default filter if not explicitly overridden
	if ca.RoomFilter == "" {
		ca.RoomFilter = roomID
	}

	result, err := h.searchEngine.SemanticSearch(ca.Query, ca.RoomFilter, ca.UserFilter)
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
