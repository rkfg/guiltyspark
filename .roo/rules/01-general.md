# Важные детали по Go

1. При работе и исследовании тебе могут понадобиться исходники сторонних библиотек Go. Не рассчитывай на обычные пути. Используй `go env GOMODCACHE` в качестве корня окружения Go. Например:
  - НЕПРАВИЛЬНО: `find /home/user/go/pkg/mod/example.com -name "*.go"`
  - ПРАВИЛЬНО: `find "$(go env GOMODCACHE)/example.com" -name "*.go"`

2. При использовании strings.Builder часто бывает нужен fmt.Sprintf и подобные функции вместе с WriteString. Используй fmt.Fprintf напрямую вместо этого. Например:
  - НЕПРАВИЛЬНО: `sb.WriteString(fmt.Sprintf("text %s %d", "123", 123))`
  - ПРАВИЛЬНО: `fmt.Fprintf(&sb, "text %s %d", "123", 123)`