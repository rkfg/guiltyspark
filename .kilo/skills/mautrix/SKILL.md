---
name: mautrix
description: Research notes for the mautrix Matrix Go SDK (maunium.net/go/mautrix v0.27.0) — key types, methods, response types, event types, ID types, sync patterns, E2EE/crypto integration with cryptohelper and OlmMachine. Use when working with mautrix client, pagination, room history, link previews, E2EE, or any Matrix API calls through the mautrix library.
---

# Mautrix (maunium.net/go/mautrix) Research Notes

## Version
`v0.27.0` — imported as `maunium.net/go/mautrix`

## Module Cache Location
`$(go env GOPATH)/pkg/mod/maunium.net/go/mautrix@v0.27.0/`

---

## Key Types from `mautrix` Package

### `Direction`
```go
type Direction rune
```
**No DirB/DirF constants** — use raw rune literals:
- `'b'` — backward (newest-first)
- `'f'` — forward (oldest-first)

Usage: `mautrix.Direction('b')`

### `FilterPart`
```go
type FilterPart struct { /* ... */ }
```
Pointer type: `*FilterPart` — can be `nil`.

---

## Key Methods on `*mautrix.Client`

### `Messages` — paginate room history
```go
func (cli *Client) Messages(
    ctx context.Context, roomID id.RoomID,
    from, to string, dir Direction,
    filter *FilterPart, limit int,
) (*RespMessages, error)
```

### `Whoami` — resolve device ID from access token
```go
func (cli *Client) Whoami(ctx context.Context) (*RespWhoami, error)
```
Returns `resp.DeviceID` (id.DeviceID) and `resp.UserID` (id.UserID).
**Critical:** `NewClient()` sets UserID and AccessToken but NOT DeviceID. `Login()` or `Whoami()` populates DeviceID. Use `Whoami()` before initializing cryptohelper when you only have an access token.

### `Login`
```go
func (cli *Client) Login(ctx context.Context, req *ReqLogin) (*RespLogin, error)
```
If `req.StoreCredentials == true`, sets `cli.DeviceID`, `cli.AccessToken`, `cli.UserID` from response.

### `ResolveAlias`
```go
func (cli *Client) ResolveAlias(ctx context.Context, alias id.RoomAlias) (*RespAliasCreate, error)
```
Returns `resp.RoomID` as `string` — cast to `id.RoomID(resp.RoomID)`.

### `JoinedMembers`, `JoinRoom`, `LeaveRoom`, `SendMessageEvent`, `StopSync`
Same signatures as documented below.

### `GetURLPreview` (DEPRECATED for preview_url)
```go
func (cli *Client) GetURLPreview(ctx context.Context, url string) (*event.LinkPreview, error)
```
**Not used** — the bot uses a direct HTTP request to `/_matrix/client/v1/media/preview_url` with double URL encoding for reliability.

---

## Key Types from `responses.go`

### `RespMessages`
```go
type RespMessages struct {
    Start string
    Chunk []*event.Event
    State []*event.Event
    End   string
}
```
- `Chunk` — events ordered by timestamp
- `End` — pagination token for next page. Empty = no more pages.
- When paginating with `dir='b'` (backward), events are **newest-first**. Reverse client-side.

### `RespWhoami`
```go
type RespWhoami struct {
    UserID   id.UserID
    DeviceID id.DeviceID
}
```

### `RespLogin`
```go
type RespLogin struct {
    AccessToken string
    DeviceID    id.DeviceID
    UserID      id.UserID
    RefreshToken string
    ExpiresInMS  int64
}
```

---

## Request Types (requests.go)

### `ReqLogin`
```go
type ReqLogin struct {
    Type                     AuthType
    Identifier               UserIdentifier
    Password                 string          // optional
    Token                    string          // optional
    DeviceID                 id.DeviceID     // optional
    InitialDeviceDisplayName string          // creates new device if set
    RefreshToken             bool
    StoreCredentials         bool            // writes AccessToken/DeviceID to client
    StoreHomeserverURL       bool
}
```

### `ReqJoinRoom`
```go
type ReqJoinRoom struct {
    Reason string
    Via    []string
}
```

### `ReqQueryKeys`
```go
type ReqQueryKeys struct {
    DeviceKeys map[id.UserID]DeviceIDList
}
```

---

## Key Methods on `*mautrix.Client` (continued)

### `UploadKeys`
```go
func (cli *Client) UploadKeys(ctx context.Context, req *ReqUploadKeys) (*RespUploadKeys, error)
```
Used by cryptohelper internally to upload Olm account keys, device keys, and one-time keys to the server.

### `CreateDeviceMSC4190`
```go
func (cli *Client) CreateDeviceMSC4190(ctx context.Context, deviceID id.DeviceID, initialDisplayName string) error
```
For appservice bots creating devices via MSC4190.

### `LoginAs` field on `CryptoHelper`
```go
type CryptoHelper struct {
    LoginAs *mautrix.ReqLogin  // if set, cryptohelper will login during Init()
    // ...
}
```
If `LoginAs` is set, `cryptohelper.Init()` will call `client.Login()` before setting up the OlmMachine. The `StoreCredentials` flag (set internally) ensures `client.AccessToken` and `client.DeviceID` are populated from the login response.

---

## Key Types from `event` Package

### Event Types
```go
event.EventMessage      // "m.room.message"
event.EventEncrypted    // "m.room.encrypted"
event.StateMember       // "m.room.member"
event.AccountDataDirectChats.Type  // "m.direct"
event.FormatHTML
```

### `MessageEventContent`
```go
body.Body        string
body.MsgType     string
body.URL         id.ContentURI
body.FileName    string
body.Info.MimeType string
body.Format      string            // "org.matrix.custom.html"
body.FormattedBody string
```

### `EncryptedEventContent`
```go
content.Algorithm   id.Algorithm  // "m.megolm.v1.aes-sha2" or "m.olm.v1.curve25519-aes-sha2"
content.SenderKey   id.SenderKey
content.SessionID   id.SessionID
content.Ciphertext  map[string]string  // curve25519 key -> ciphertext
```
Access via `ev.Content.AsEncrypted()`.

---

## ID Types

Import: `maunium.net/go/mautrix/id`

```go
id.UserID(string)
id.RoomID(string)
id.RoomAlias(string)
id.EventID(string)
id.DeviceID(string)
id.SenderKey(string)
id.SessionID(string)
id.ContentURI(string)
id.Algorithm(string)
id.DeviceKeyID(id.KeyAlgorithm, id.DeviceID)
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

For E2EE, the state store must implement `crypto.StateStore` (SQLStateStore from `maunium.net/go/mautrix/sqlstatestore`).

---

## Syncer

```go
syncer := b.client.Syncer.(*mautrix.DefaultSyncer)
syncer.OnEventType(event.EventMessage, handler)
syncer.OnEventType(event.StateMember, handler)
syncer.OnEventType(event.EventEncrypted, handler)  // for E2EE
syncer.OnSync(handler)
```

`OnSync` handler signature:
```go
func(ctx context.Context, resp *mautrix.RespSync, since string) bool
```
`RespSync` fields: `resp.Rooms.Invite`, `resp.Rooms.Join`, `resp.Rooms.Leave`, `resp.Pos`

### E2EE Sync Handlers (registered by cryptohelper.Init())
After `cryptoHelper.Init()`, these handlers are automatically registered on the syncer:
```go
syncer.OnSync(helper.mach.ProcessSyncResponse)
syncer.OnEventType(event.StateMember, helper.mach.HandleMemberEvent)
syncer.OnEventType(event.EventEncrypted, helper.HandleEncrypted)
```
- `ProcessSyncResponse` — processes to-device events (room_key, m.room_key_request), triggers key fetching
- `HandleMemberEvent` — tracks device list changes
- `HandleEncrypted` — decrypts `m.room.encrypted` events and re-dispatches as decrypted `m.room.message`

---

## E2EE / Crypto — cryptohelper

### Overview
Mautrix provides `cryptohelper.CryptoHelper` (in `maunium.net/go/mautrix/crypto/cryptohelper`) to handle E2EE. It manages:
- Olm account (device identity keys)
- Megolm sessions (room key encryption)
- Key exchange and sharing
- Decryption of incoming encrypted events

### Setup Pattern (with password login for new device)
```go
// 1. Create logger
clientLog := zerolog.New(os.Stdout).With().Timestamp().Logger()

// 2. Create client without access token (cryptohelper will login)
mclient, err := mautrix.NewClient(cfg.Homeserver, userID, "")
mclient.Log = clientLog

// 3. Create crypto helper — pass DB path as string
cryptoHelper, err := cryptohelper.NewCryptoHelper(
    mclient,
    []byte(cfg.E2EE.PickleKey),
    cfg.E2EE.DBPath,  // path as string — cryptohelper creates stores internally
)
cryptoHelper.DBAccountID = "guiltyspark-bot"

// 4. Set LoginAs for password login with new device
cryptoHelper.LoginAs = &mautrix.ReqLogin{
    Type:                   mautrix.AuthTypePassword,
    Identifier:             mautrix.UserIdentifier{Type: mautrix.IdentifierTypeUser, User: cfg.Bot.UserID},
    Password:               cfg.E2EE.LoginPassword,
    InitialDeviceDisplayName: "guiltyspark-search-bot",
}

// 5. Init (this calls Login, creates OlmMachine, sets up sync handlers)
ctx := context.Background()
if err := cryptoHelper.Init(ctx); err != nil {
    // handle error
}

// 6. After Init, cryptohelper has set client.AccessToken and client.DeviceID
```

### Setup Pattern (fallback with existing access token)
```go
// Client already has access token from config
mclient, err := mautrix.NewClient(cfg.Homeserver, userID, cfg.Bot.AccessToken)
mclient.Log = clientLog

// Resolve device ID via Whoami (NewClient doesn't set DeviceID)
whoamiResp, err := mclient.Whoami(ctx)
mclient.DeviceID = whoamiResp.DeviceID

// Crypto helper with DB path as string (no LoginAs)
cryptoHelper, err := cryptohelper.NewCryptoHelper(mclient, []byte(pickleKey), dbPath)
cryptoHelper.DBAccountID = "guiltyspark-bot"
// No LoginAs — cryptohelper won't try to login

// Init
if err := cryptoHelper.Init(ctx); err != nil {
    // May fail with "olm account is not marked as shared" if device
    // was registered via Element (non-shared account with keys on server)
    // Fix: manually register sync listeners + ShareKeys (see below)
}
```

### NewCryptoHelper signatures
```go
func NewCryptoHelper(cli *mautrix.Client, pickleKey []byte, store any) (*CryptoHelper, error)
```
`store` can be:
- `string` — DB path, cryptohelper creates SQLStateStore + SQLCryptoStore internally
- `*dbutil.Database` — existing DB, same behavior
- `crypto.Store` — custom crypto store (must also set StateStore on client)

### Init() flow
1. Upgrades state store (SQLStateStore) and crypto store (SQLCryptoStore)
2. If `LoginAs` is set → calls `client.Login()` → sets `client.DeviceID` and `client.AccessToken`
3. If `LoginAs` is nil but `client.DeviceID` is set → continues
4. Creates `OlmMachine` with crypto store and state store
5. Loads Olm account from crypto store (creates new if not exists)
6. Calls `verifyDeviceKeysOnServer()` — checks keys on server match local account
7. If syncer implements `ExtensibleSyncer`:
   - Registers `OnSync(ProcessSyncResponse)`
   - Registers `OnEventType(StateMember, HandleMemberEvent)`
   - Registers `OnEventType(EventEncrypted, HandleEncrypted)`

### verifyDeviceKeysOnServer() — common failure mode
Fails with **"olm account is not marked as shared, but there are keys on the server"** when:
- Device was created via Element (or regular registration, not E2EE)
- Server has ed25519 keys for the device
- Local Olm account is NOT marked as shared
- This is a consistency check — non-shared devices shouldn't have keys on server

**Workaround** (when device was pre-registered via Element):
```go
initErr := cryptoHelper.Init(ctx)
if initErr != nil && strings.Contains(initErr.Error(), "not marked as shared") {
    // Register sync listeners manually (Init didn't complete)
    syncer := client.Syncer.(mautrix.ExtensibleSyncer)
    syncer.OnSync(cryptoHelper.Machine().ProcessSyncResponse)
    syncer.OnEventType(event.StateMember, cryptoHelper.Machine().HandleMemberEvent)
    if _, ok := client.Syncer.(mautrix.DispatchableSyncer); ok {
        syncer.OnEventType(event.EventEncrypted, cryptoHelper.HandleEncrypted)
    }
    // Try ShareKeys to overwrite old Element keys with new Olm keys
    if err := cryptoHelper.Machine().ShareKeys(ctx, -1); err != nil {
        if strings.Contains(err.Error(), "already exists") || strings.Contains(err.Error(), "M_UNKNOWN") {
            // Keys already on server, continue
        } else {
            // Fatal error
        }
    }
}
```

### ShareKeys
```go
func (mach *OlmMachine) ShareKeys(ctx context.Context, currentOTKCount int) error
```
Uploads Olm account keys, device keys (ed25519), and one-time keys to the server. Also marks the account as `Shared = true`. Only works if device has no keys on server (fresh device). If keys already exist, returns `M_UNKNOWN` / "already exists" error.

### Machine()
```go
func (helper *CryptoHelper) Machine() *crypto.OlmMachine
```
Returns the internal OlmMachine. **Panics if called before Init() completes** (mach is nil). Safe to call after Init fails (mach is created before verifyDeviceKeysOnServer check).

### Close()
```go
func (helper *CryptoHelper) Close() error
```
Closes the managed database (if any). Call from `Bot.Stop()`.

---

## E2EE Crypto Store SQLite Schema

The crypto store (SQLCryptoStore) stores all E2EE state in SQLite. Key tables:

```sql
-- Olm account (device identity) — MUST exist for E2EE
CREATE TABLE crypto_account (
    account_id TEXT PRIMARY KEY,
    device_id TEXT NOT NULL,
    shared BOOLEAN NOT NULL,
    sync_token TEXT NOT NULL,
    account BLOB NOT NULL,
    key_backup_version TEXT DEFAULT ''
);

-- Device identities
CREATE TABLE crypto_device (
    user_id TEXT, device_id TEXT,
    identity_key CHAR(43), signing_key CHAR(43),
    trust SMALLINT, deleted BOOLEAN, name TEXT,
    PRIMARY KEY (user_id, device_id)
);

-- Megolm inbound sessions (decrypted room keys)
CREATE TABLE crypto_megolm_inbound_session (
    account_id TEXT, session_id CHAR(43),
    sender_key CHAR(43), signing_key CHAR(43),
    room_id TEXT, session BLOB,
    PRIMARY KEY (account_id, session_id)
);

-- Megolm outbound sessions (sending)
CREATE TABLE crypto_megolm_outbound_session (
    account_id TEXT, room_id TEXT,
    session_id CHAR(43) UNIQUE,
    session BLOB, shared BOOLEAN,
    PRIMARY KEY (account_id, room_id)
);

-- Olm sessions (to-device)
CREATE TABLE crypto_olm_session (
    account_id TEXT, session_id CHAR(43),
    sender_key CHAR(43), session BLOB,
    PRIMARY KEY (account_id, session_id)
);

-- Cross-signing keys
CREATE TABLE crypto_cross_signing_keys (
    user_id TEXT, usage TEXT,
    key CHAR(43), first_seen_key CHAR(43),
    PRIMARY KEY (user_id, usage)
);
```

**Critical:** The `crypto_account` table holds the Olm device identity. If deleted, cryptohelper creates a new account with new keys on the next login. The device ID (from `crypto_device`) is reused — so the same device gets a fresh Olm account.

**Clearing Olm account:**
```sql
DELETE FROM crypto_account;  -- forces new Olm account on next login
-- Device ID from crypto_device is NOT cleared, so same device is reused
```

---

## Common Patterns

### Context with timeout
```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
```

### Direction (no DirB/DirF constants)
```go
mautrix.Direction('b')  -- backward, newest-first
mautrix.Direction('f')  -- forward, oldest-first
```

### Room keys in config
Config entries for rooms may use room alias (`#alias:server`) or room ID (`!roomid:server`). Always resolve aliases:
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
**Always call `ev.Content.ParseRaw(ev.Type)`** before casting `Content.Parsed`. Without this, the type assertion always fails silently.

### Stale page counting
A page is "stale" if it produced zero newly indexed documents. Scan stops when `stalePages >= StalePagesThreshold` (default 3).

### `isDirectMessage` fallback
When `m.direct` is set but the room is NOT in it, **don't return false immediately** — fall back to member count check. Unencrypted DMs may not be synced to `m.direct` yet.

### Link preview
Use direct HTTP request to `/_matrix/client/v1/media/preview_url` with double URL encoding instead of `GetURLPreview` for reliability with non-ASCII characters.

---

## E2EE — Common Pitfalls & Solutions

### Problem: "olm account is not marked as shared, but there are keys on the server"
**Cause:** Device was created via Element/registration (non-shared account) but cryptohelper creates a fresh Olm account with `shared=false`. Server has ed25519 keys from Element.

**Solution:** Use `LoginAs` with password to create a new device with proper shared Olm account:
```go
cryptoHelper.LoginAs = &mautrix.ReqLogin{
    Type: mautrix.AuthTypePassword,
    Identifier: mautrix.UserIdentifier{Type: mautrix.IdentifierTypeUser, User: "..."},
    Password: "password",
    InitialDeviceDisplayName: "bot-name",
}
```

### Problem: New Olm account created on every restart (accumulating sessions)
**Cause:** `DELETE FROM crypto_account` runs on every startup → cryptohelper creates a new Olm account each time.

**Fix:** Only delete if no account exists:
```go
var count int
db.QueryRow("SELECT COUNT(*) FROM crypto_account").Scan(&count)
if count == 0 {
    db.Exec("DELETE FROM crypto_account")  // force fresh account
}
// else: reuse existing account across restarts
```

### Problem: "One time key signed_curve25519 already exists"
**Cause:** `ShareKeys` called when one-time keys already uploaded from previous attempt.

**Fix:** Check error message — if "already exists", skip ShareKeys (keys are already there, sync listeners will be registered).

### Problem: Client AccessToken is empty after Init
**Cause:** `LoginAs` with `StoreCredentials = true` writes to `client.AccessToken` and `client.DeviceID` internally. But if Init fails before reaching that code, token is empty.

**Fix:** Check `client.AccessToken` after Init; if empty, extract from `LoginAs.Token` or `LoginAs.AccessToken` (depending on auth type used).

### Problem: Encrypted messages not decrypting — "olm event doesn't contain ciphertext for this device"
**Cause:** Old device keys cached in Element. When cryptohelper creates a new Olm account, Element still encrypts `m.room_key` with the OLD Olm key.

**Solution:** Use a fresh device via `LoginAs` with password (not ShareKeys workaround on existing device). Element installs a new Olm session with the new device's key.

### Problem: Device ID not set after NewClient
**Cause:** `NewClient(homeserver, userID, accessToken)` sets UserID and AccessToken but NOT DeviceID. Only `Login()` or `Whoami()` populates it.

**Fix:** Call `Whoami()` before `cryptoHelper.Init()`:
```go
whoamiResp, _ := client.Whoami(ctx)
client.DeviceID = whoamiResp.DeviceID
```

---

## References

- Module source: `https://github.com/mautrix/go`
- `client.go` — all client methods
- `responses.go` — response types (RespMessages, RespWhoami, RespLogin, etc.)
- `requests.go` — request types (ReqLogin, ReqJoinRoom, etc.)
- `sync.go` — sync logic (OnEventType, OnSync, Dispatch)
- `statestore.go` — state store interface
- `sqlstatestore/statestore.go` — SQL-backed state store
- `event/` — event types and content structs
- `id/` — ID types
- `crypto/machine.go` — OlmMachine
- `crypto/cryptohelper/cryptohelper.go` — CryptoHelper (Init, HandleEncrypted, ShareKeys, Machine, Close)
- `crypto/account.go` — OlmAccount (getInitialKeys, getOneTimeKeys)
