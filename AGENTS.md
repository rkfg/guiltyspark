# Guiltyspark — Matrix Search Bot

## Build

```bash
go build -tags vectors ./cmd/bot/
```

**Важно:** флаг `-tags vectors` обязателен. Без него не компилируется `AddKNN` и векторные поля в `bleve_client.go` — это тег из `github.com/blevesearch/bleve/v2` для поддержки векторного поиска через FAISS.

## Runtime dependencies

- **ImageMagick** (`convert`) — для конвертации изображений в JPG и ресайза
- **FAISS** — собирается через скрипт build-faiss.sh, на выходе будет `.deb` в `output/libfaiss-c_1.13.2-bleve-1_amd64.deb`
- **Bleve index** хранится в `bot-data/index.bleve/`
- **Deferred data** хранится в `bot-data/deferred.json` (отложенные изображения/тексты для LLM обработки)

## Architecture

### Indexing pipeline

Текст индексируется **немедленно** в Bleve через `IndexTextFn`. Embedding (вектор) текста откладывается на ночное время (настраивается через `delayed_embed_hour`/`delayed_embed_minute` в config.yaml).

Изображения обрабатываются по расписанию:
1. При получении — добавляются в `deferredImages` и сохраняются в `deferred.json`
2. При скачивании — конвертируются, ресайзятся, кэшируются
3. Embedding создаётся в ночное время через VLM в `ProcessDeferredFn`

**Deduplication:** по `EventID` для изображений и текстов в `deferredImages`/`deferredTextEmbed`.

### Bleve index

Используется нативный kNN с FAISS backend. Векторное поле хранится как `[]float32` в структуре `IndexedDocument`. При индексации используется `IndexDocumentStruct()` (struct-based), а не `IndexDocument()` (map-based).

**Keyword mapping** для ID полей (`room_id`, `user_id`, `event_id`) — используется `bleve.NewKeywordFieldMapping()` для предотвращения токенизации.

### Search

- `!search` — точный текстовый поиск через `DisjunctionQuery` + `ConjunctionQuery` (поля `text`, `image_desc`, фильтр по `room_id` через `TermQuery`)
- `!semantic` — только семантический поиск (vector similarity)
- `!stats` — статистика индекса
- Поддержка фильтра `--user <user_id>`
- Команды работают **только в DM** (прямых сообщениях с ботом)
- Для поиска в конкретной комнате укажите ссылку/алиас в любом месте аргументов:
  ```
  !search слово #roomname:server.org
  !search semantic query !roomid:server.org
  ```
  Ссылка автоматически удаляется из текста запроса и резолвится в room ID.

## Configuration

`config.yaml.sample` — полный пример в репозитории. Ключевые поля:

- `indexing.delayed_embed_hour/minute` — время ночной LLM-обработки (по умолчанию 05:00)
- `search.vector_dimensions` — размерность векторов (обычно 4096 для Qwen3)
- `image_processing.*` — параметры ImageMagick
- `link_preview.enabled` — включать извлечение превью для ссылок в сообщениях
- `link_preview.timeout` — таймаут запроса превью (по умолчанию 5s)
- `link_preview.max_urls` — макс. количество ссылок на сообщение (по умолчанию 5)

## Known quirks

- `chatResponseString` — fallback для моделей, возвращающих контент как строку, а не массив content items
- `lastImageDesc` — кэш последнего описания изображения (публичный доступ через `LastImageDescription()`)
- Grace period: сообщения до `startTime - gracePeriod` игнорируются
- Команды не индексируются
- `sync.Mutex` для данных (`mu`) и `sync.Mutex` для save операций (`saveMu`) — раздельные мьютексы для избежания dead-lock
- Persistence: `deferred.json` сохраняется при каждом сообщении и очищается после обработки
- Link preview: для сообщений с URL запрашивается превью через API Matrix `GET /_matrix/media/v3/preview_url` (Synapse). Превью добавляются к тексту сообщения как `preview: [title] - description`. Неудавшиеся запросы игнорируются.
- Команды работают только в DM — это обеспечивает приватность результатов поиска
- Room alias resolution: `#alias:server` резолвится через `GET /_matrix/client/v3/rooms/{alias}` — через HTTP запрос к серверу
- DM проверка: через `m.direct` account data — бот проверяет, что текущая комната есть в списке прямых сообщений, либо если в ней менее 3 участников
