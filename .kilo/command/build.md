---
name: build
description: Собрать бота и проверить через go vet, бинарник сохраняется как bot-bin
---

## Сборка бота

```bash
go build -tags vectors -o bot-bin ./cmd/bot/ && go vet -tags vectors ./...
```

**Обязательно:** флаг `-tags vectors` нужен для векторного поиска FAISS.
Бинарник `bot-bin` остаётся в корне проекта после сборки.
