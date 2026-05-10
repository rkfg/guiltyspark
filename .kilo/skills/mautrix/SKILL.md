---
name: mautrix
description: Research notes for the mautrix Matrix Go SDK (maunium.net/go/mautrix v0.27.0) — key types, methods, response types, event types, ID types, sync patterns. Use when working with mautrix client, pagination, room history, link previews, or any Matrix API calls through the mautrix library.
---

# Mautrix (maunium.net/go/mautrix) Research Notes

## Version
`v0.27.0` — imported as `maunium.net/go/mautrix`

## Module Cache Location
`$(go env GOPATH)/pkg/mod/maunium.net/go/mautrix@v0.27.0/`
(verify with: `go get maunium.net/go/mautrix@v0.27.0`)

---

## Key Types from `mautrix` Package

### `Direction`
```go
type Direction rune
```
**No DirB/DirF constants in mautrix** — use raw rune literals directly:
- `'b'` — backward (fetches from newest toward oldest, i.e., going back in time)
- `'f'` — forward (fetches from oldest toward newest, i.e., going forward in time)

Usage: `mautrix.Direction('b')`

### `FilterPart`
```go
type FilterPart struct { /* ... */ }
```
Pointer type: `*FilterPart` — can be `nil` to skip filtering.

---

## Key Methods on `*mautrix.Client`

### `Messages` — paginate room history
```go
func (cli *Client) Messages(
    ctx context.Context,
    roomID id.RoomID,
    from, to string,   // pagination tokens
    dir Direction,      // 'b' or 'f'
    filter *FilterPart, // nil for no filter
    limit int,          // e.g. 100
) (*RespMessages, error)
```

### `GetURLPreview` — fetch link preview
```go
func (cli *Client) GetURLPreview(
    ctx context.Context,
    url string,
) (*event.LinkPreview, error)
```

### `ResolveAlias` — resolve room alias to room ID
```go
func (cli *Client) ResolveAlias(
    ctx context.Context,
    alias id.RoomAlias,
) (*mautrix.RespAliasCreate, error)
```
Returns `resp.RoomID` as `string` — cast to `id.RoomID(resp.RoomID)`.

### `JoinedMembers`
```go
func (cli *Client) JoinedMembers(
    ctx context.Context,
    roomID id.RoomID,
) (*mautrix.RespJoinedMembers, error)
```

### `JoinRoom`
```go
func (cli *Client) JoinRoom(
    ctx context.Context,
    roomID string,
    req *mautrix.ReqJoinRoom,
) (*mautrix.RespJoinRoom, error)
```

### `LeaveRoom`
```go
func (cli *Client) LeaveRoom(
    ctx context.Context,
    roomID id.RoomID,
) error
```

### `SendMessageEvent`
```go
func (cli *Client) SendMessageEvent(
    ctx context.Context,
    roomID id.RoomID,
    eventType id.EventType,
    content any,
) (id.EventID, error)
```

### `StopSync`
```go
func (cli *Client) StopSync()
```
Called from `Bot.Stop()`.

---

## Key Types from `responses.go`

### `RespMessages`
```go
type RespMessages struct {
    Start string         `json:"start"`
    Chunk []*event.Event `json:"chunk"`
    State []*event.Event `json:"state"`
    End   string         `json:"end,omitempty"`
}
```
- `Chunk` — slice of events ordered by timestamp
- `End` — pagination token for next page. Empty = no more pages.
- When paginating with `dir='b'` (backward), events are returned **newest-first**. Reverse client-side for chronological order.

---

## Event Types

Import: `maunium.net/go/mautrix/event`

Key constants:
```go
event.EventMessage      // "m.room.message"
event.MsgText           // "m.text"
event.MsgImage          // "m.image"
event.StateMember       // "m.room.member"
event.AccountDataDirectChats.Type  // "m.direct"
event.FormatHTML
```

`event.MessageEventContent` fields used:
```go
body.Body        string
body.MsgType     string
body.URL         id.ContentURI   // for images
body.FileName    string
body.Info.MimeType string
body.Format      string            // "org.matrix.custom.html"
body.FormattedBody string
```

---

## ID Types

Import: `maunium.net/go/mautrix/id`

```go
id.UserID(string)    // or id.UserID(b.cfg.Bot.UserID)
id.RoomID(string)    // or id.RoomID(roomID)
id.RoomAlias(string) // e.g. id.RoomAlias("#alias:server")
id.EventID(string)
id.EventEventType
```

---

## State Store

```go
b.client.StateStore = mautrix.NewMemoryStateStore()
```
Methods:
```go
b.client.StateStore.SetMembership(ctx, roomID, userID, event.MembershipJoin)
```

---

## Syncer

```go
syncer := b.client.Syncer.(*mautrix.DefaultSyncer)
syncer.OnEventType(event.EventMessage, handler)
syncer.OnEventType(event.StateMember, handler)
syncer.OnSync(handler)
```

`OnSync` handler signature:
```go
func(ctx context.Context, resp *mautrix.RespSync, since string) bool
```
`RespSync` fields: `resp.Rooms.Invite`, `resp.Rooms.Join`

---

## Common Patterns

### Context with timeout
```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
```

### Direction (no DirB/DirF constants)
`mautrix` defines `Direction` as `type Direction rune` — no named constants. Use rune literals directly:
- `mautrix.Direction('b')` — backward, newest-first
- `mautrix.Direction('f')` — forward, oldest-first

### Room keys in config can be aliases or IDs
Config entries for rooms may use either a room alias (`#alias:server`) or a room ID (`!roomid:server`). API methods like `Messages()` require a room ID. Passing an alias string directly will return `M_FORBIDDEN` (403) because the bot is not a member of the room identified by that raw string. Always resolve aliases first:
```go
func resolveRoomKeyToID(key string) (id.RoomID, error) {
    if strings.HasPrefix(key, "#") {
        alias := id.RoomAlias(key)
        roomID, err := b.client.ResolveAlias(ctx, alias)
        if err != nil { return "", err }
        return id.RoomID(roomID.RoomID), nil
    }
    return id.RoomID(key), nil
}
```

### `Messages()` returns raw events — `Content.Parsed` is NOT populated
`b.client.Messages()` returns events as raw JSON from the server API. Unlike events received via `/sync` where `ev.Content.Parsed` is pre-filled by `DefaultSyncer`, events from `Messages()` have `Parsed == nil`. **Always call `ev.Content.ParseRaw(ev.Type)` before casting `Content.Parsed`** to any content struct. Without this, the type assertion always fails silently.

### Stale page counting — stop after N consecutive pages with no new documents indexed
When scanning room history backwards, a page counts as "stale" if it produced zero newly indexed documents (`hasNewEvents == false`). This includes pages with only already-indexed events, bot messages, commands, non-message events, or events before cutoff. The scan stops when `stalePages >= StalePagesThreshold` (default 3).

**Critical:** `hasNewEvents` must reflect actual dedup-checked additions, not just "we sent something to a channel". For images, use `BatchIndexer.QueueImage()` (sync, returns bool) — `OnImageMessage()` is async (channel) and always "succeeds", so it can't distinguish already-queued images from new ones. `QueueImage` + `isIndexed` (local O(1) cache in BleveClient) provides correct dedup without O(n) linear scans.

### API method name
The method is `Messages()` not `RoomMessages()`. `RoomMessages` exists only in `synapseadmin` package as an alias to `RespMessages`.

---

## References

- Module source: `https://github.com/mautrix/go`
- `client.go` — all client methods
- `responses.go` — response types
- `requests.go` — request types (Direction, FilterPart, etc.)
- `sync.go` — sync logic
- `statestore.go` — state store interface
- `event/` — event types and content structs
- `id/` — ID types (UserID, RoomID, etc.)
