---
name: go-details
description: Go-specific development patterns and gotchas — GOMODCACHE paths instead of hardcoded paths, strings.Builder + fmt.Fprintf pattern. Use when writing Go code, debugging build issues, or researching Go library sources.
---

# Important Go Details

1. When working with or researching Go third-party libraries, you'll need their source code. Do not rely on hardcoded paths. Use `go env GOMODCACHE` as the Go environment root. For example:
   - WRONG: `find /home/user/go/pkg/mod/example.com -name "*.go"`
   - CORRECT: `find "$(go env GOMODCACHE)/example.com" -name "*.go"`

2. When using `strings.Builder`, you often need `fmt.Sprintf`-style formatting with `WriteString`. Use `fmt.Fprintf` directly instead. For example:
   - WRONG: `sb.WriteString(fmt.Sprintf("text %s %d", "123", 123))`
   - CORRECT: `fmt.Fprintf(&sb, "text %s %d", "123", 123)`
