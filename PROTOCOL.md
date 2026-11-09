# PR Radar Hub — protocol v1

This is the reference for writing the hub client inside PR Radar. It is meant to be
enough on its own: everything the client needs to know about the hub is here.

**What the hub does:** it tells every connected radar which teammates have their radar
running, with how many tabs, on which version and whether they are available for a
review or focused. It also relays two small person-to-person messages between online
teammates: a kudos 🎉 and the offer of a PR Radar theme. Nothing else.

**What the hub never receives:** no pull request, no repository name, no PR title, no
GitHub token of the user. The only identity proof is a one-time GitHub sign-in that
grants no permission at all (see [Authentication](#authentication)).

## Guarantees

1. **The radar never needs the hub.** No hub answer is required for the radar to work.
   If the hub is down, slow or refuses the radar, the board behaves exactly as without
   it; only the presence feature is missing.
2. **Unknown message types are ignored**, on both sides, without error and without
   closing the connection. Unknown fields in a known message are ignored too.
3. **Within protocol 1, the hub only ever adds** message types and fields. It never
   removes a field, renames it or changes its meaning. A breaking change gets
   protocol 2, and the hub then says so with `protocol_unsupported`.
4. **Messages are small**: one JSON object per WebSocket text frame. A radar sends at
   most 16 KiB per frame. The hub's frames stay under 16 KiB too, except a relayed
   `theme_offer`, which adds up to 512 bytes to what the sender sent.

## Connection

| | |
| --- | --- |
| URL | `wss://radar-hub.matthv.com/ws` (configured on the radar as `PR_RADAR_HUB_URL`) |
| Who connects | the **radar server process** (`server.js`), once per running radar — not the browser tabs |
| Frames | text frames, each one a JSON object with a string `type` |
| Keepalive | the hub sends a WebSocket **ping** every 30 s; a connection that has not answered with a **pong** for 75 s is dropped and its radar shown offline |

The keepalive uses native WebSocket control frames. Node's built-in `WebSocket`
(global since Node 22) answers pings by itself: the client has nothing to do.

> **Node version.** PR Radar requires Node 22.4+ (`engines.node` is `>=22.4`), the first
> release where the global `WebSocket` is stable. The client needs no dependency.

### Lifecycle

```
radar                                   hub
  | ---- open wss://…/ws --------------> |
  | ---- hello ------------------------> |   within 10 s of opening
  |                                      |
  |   known hubToken:                    |
  | <--- welcome ----------------------- |
  | <--- presence ---------------------- |
  |                                      |
  |   no (or no valid) hubToken:         |
  | <--- auth_required (user code) ----- |   user signs in on github.com
  | <--- welcome (with new hubToken) --- |
  | <--- presence ---------------------- |
  |                                      |
  | ---- status (on change) -----------> |
  | <--- presence (on any change) ------ |
  |                                      |
  | ---- kudos / theme_offer ----------> |   relayed to the recipient's radars,
  | <--- undelivered ------------------- |   or undelivered if they are offline
  |                                      |
  | <--- error + close ----------------- |   on refusal, see the error table
```

## Messages from the radar

### `hello`

The first message, and the only one accepted before `welcome`. Must arrive within
**10 s** of the connection opening.

```json
{
  "type": "hello",
  "protocol": 1,
  "hubToken": "rh_Q2x1Z…43 chars",
  "instanceId": "5f0c3b1e-8d0a-4f5e-9a51-3c6c1f2e7b90",
  "version": { "commit": "129a21f3c4d5e6f708192a3b4c5d6e7f80912a3b", "behind": 2 },
  "tabs": 1,
  "availability": "available"
}
```

| Field | Type | Notes |
| --- | --- | --- |
| `protocol` | integer | `1` |
| `hubToken` | string, optional | The token from a previous `welcome`. Omit it (or send `null`) to sign in |
| `instanceId` | string, 8–64 chars `[A-Za-z0-9_-]` | Random, generated once per machine and kept. Identifies this radar among the user's radars; never shown to other users |
| `version.commit` | string or `null` | Full SHA of PR Radar's `HEAD`, `null` if unknown (zip download, no git) |
| `version.behind` | integer or `null` | Commits behind `origin/main`, as computed by `update.js`; `null` when not checked yet or the check failed |
| `tabs` | integer ≥ 0 | Board tabs currently open. `0` still means *online*: the radar runs |
| `availability` | `"available"`, `"focus"` or `null`, optional | The user's state, see below |

A radar in **demo mode** (`PR_RADAR_DEMO`) never connects to the hub.

#### `availability`

The user's state, set in one click on any of their radars and shown to the others:

- `"available"`: available for a review;
- `"focus"`: focused, better not to disturb;
- `null`: no state.

The value belongs to the **login**, not to one radar: the hub keeps the last value
received from any of the user's radars and shows it on their `presence` entry. Any
other value counts as `null` (no `bad_message`). It lives in memory only and is
forgotten when the user's last radar disconnects.

An **absent** field keeps the current value, while `null` clears it. A radar that
does not know the user's choice should therefore omit the field in `hello`, rather
than send `null` and erase what another of the user's radars set.

### `status`

Sent whenever `version`, `tabs` or `availability` changes. Same fields as in `hello`, all optional:
send what changed or everything, the hub keeps the last value of each field.

```json
{ "type": "status", "tabs": 2 }
```

```json
{ "type": "status", "availability": "focus" }
```

Debounce tab changes (about 2 s): a tab reload is a close then an open, one `status` is
enough. An availability change can go at once. More than **30 messages per minute** on
one connection, all types together, closes it with `rate_limited`.

### `kudos`

A 🎉 for an online teammate.

```json
{ "type": "kudos", "to": "bexchauveto" }
```

| Field | Notes |
| --- | --- |
| `to` | GitHub login of the recipient, `[A-Za-z0-9-]{1,39}`, any case. Otherwise `bad_message` |

- The hub sends a [`kudos`](#kudos-1) to **every** connected radar of `to`.
- If `to` has no radar connected, the sender gets [`undelivered`](#undelivered).
- A kudos to oneself is ignored.
- At most **one kudos per minute** for a given sender → recipient pair, whichever
  radars send it. The extra ones are **ignored** silently, without closing: the
  radar should disable the button for a minute rather than rely on this.

### `theme_offer`

Offers one of the user's PR Radar themes to an online teammate.

```json
{
  "type": "theme_offer",
  "to": "bexchauveto",
  "theme": { "name": "foret", "label": "Forêt", "icon": "🌲", "css": ":root { … }", "js": null }
}
```

| Field | Notes |
| --- | --- |
| `to` | GitHub login of the recipient, as for `kudos` |
| `theme` | An object. The hub checks its form only, and closes with `bad_message` if it is wrong |
| `theme.name` | `[a-z0-9-]{1,40}` |
| `theme.label` | String, at most 60 characters |
| `theme.icon` | String, at most 16 characters (an emoji) |
| `theme.css` | String, possibly empty |
| `theme.js` | String or `null` (CSS-only theme); absent counts as `null` |

Other fields of `theme` are relayed as they are.

- The whole frame stays under **16 KiB**. Check the size before sending: a bigger
  frame closes the connection with 1009.
- The hub **never stores, logs or reads** the CSS and the JS. It relays the theme and
  forgets it.
- The hub sends a [`theme_offer`](#theme_offer-1) to **every** connected radar of `to`.
- If `to` has no radar connected, the sender gets [`undelivered`](#undelivered).
- An offer to oneself is ignored.
- At most **5 offers per minute** per sender. The extra ones are ignored, without
  closing.

The hub does **not** vouch for the theme's content. A theme carries code that will run
in the recipient's radar. Only the recipient decides whether to install it, after a
clear warning in their page.

## Messages from the hub

### `auth_required`

The radar sent no `hubToken`. The hub has started a GitHub sign-in and waits for the
user to approve it. The connection stays open meanwhile.

```json
{
  "type": "auth_required",
  "userCode": "72CC-9A43",
  "verificationUri": "https://github.com/login/device",
  "expiresIn": 899
}
```

The radar shows the code and the link on the board, e.g. a discreet banner: *"See who is
online: open github.com/login/device and enter 72CC-9A43"*. Once the user approves, the
hub sends `welcome`. If they let `expiresIn` seconds pass, the hub sends
`auth_expired`.

### `welcome`

```json
{
  "type": "welcome",
  "protocol": 1,
  "login": "matthv",
  "avatarUrl": "https://avatars.githubusercontent.com/u/123456?v=4",
  "hubToken": "rh_Q2x1Z…43 chars"
}
```

`hubToken` is present **only when it was just issued**, after an `auth_required`. The
radar must store it right away (see [Storing the hub token](#storing-the-hub-token)) and
send it in every later `hello`. The hub keeps only its SHA-256 hash and cannot show it
again.

### `presence`

The full list of online teammates. Sent right after `welcome`, then on every change
(someone connects, leaves, changes tabs, version or availability), at most once per
second.
It always carries the whole list, never a diff: the radar replaces what it had.

```json
{
  "type": "presence",
  "online": [
    {
      "login": "matthv",
      "avatarUrl": "https://avatars.githubusercontent.com/u/123456?v=4",
      "since": "2026-11-06T08:41:12Z",
      "tabs": 3,
      "availability": "focus",
      "radars": [
        { "commit": "129a21f3c4d5…", "behind": 0, "tabs": 2, "since": "2026-11-06T08:41:12Z" },
        { "commit": "d7da2a3b9e8f…", "behind": 1, "tabs": 1, "since": "2026-11-06T09:02:40Z" }
      ]
    }
  ],
  "recent": [
    { "login": "jdoe", "avatarUrl": "https://avatars.githubusercontent.com/u/654321?v=4", "lastSeen": "2026-11-05T17:58:03Z" }
  ]
}
```

| Field | Notes |
| --- | --- |
| `online[]` | Members with at least one radar connected, sorted by `login`. The user's own entry is included |
| `online[].since` | When their earliest connected radar connected |
| `online[].tabs` | Sum over their radars |
| `online[].availability` | `"available"`, `"focus"` or `null`: the last value received from any of their radars |
| `online[].radars[]` | One per running radar (machine), oldest first. `behind` is the hub's best indication of who should update |
| `recent[]` | Members not online now but seen in the last 14 days, most recent first |

Timestamps are UTC, ISO 8601, second precision.

### `kudos`

```json
{
  "type": "kudos",
  "from": "matthv",
  "avatarUrl": "https://avatars.githubusercontent.com/u/123456?v=4",
  "at": "2026-11-09T16:20:00Z"
}
```

A teammate sent a kudos. Each of the recipient's radars gets one; showing the
animation in every open tab is up to the radar.

### `theme_offer`

```json
{
  "type": "theme_offer",
  "id": "Xq3b9kF0aZ1mRt7c",
  "from": "matthv",
  "avatarUrl": "https://avatars.githubusercontent.com/u/123456?v=4",
  "theme": { "name": "foret", "label": "Forêt", "icon": "🌲", "css": ":root { … }", "js": null }
}
```

`theme` is what the sender sent, as it was. `id` is random and unique per offer. Each of
the recipient's radars gets the same `id`, which lets the radar show the offer once.
There is no answer to send back: accepting or refusing stays local.

### `undelivered`

```json
{ "type": "undelivered", "kind": "kudos", "to": "bexchauveto" }
```

Sent to the sender's radar only, when the recipient of a `kudos` or a `theme_offer`
(`kind`) has no radar connected anymore. A message dropped by a limit gets no
`undelivered`.

### `error`

Sent just before the hub closes the connection. The same code is repeated as the close
reason, so a client that missed the message still knows.

```json
{ "type": "error", "code": "not_member", "message": "matthv is not an active member of ForestAdmin" }
```

`message` is for logs only; the client acts on `code`.

## Errors and close codes

| `code` | Close code | When | What the radar does |
| --- | --- | --- | --- |
| `protocol_unsupported` | 4000 | `hello.protocol` is not one the hub speaks | Stop until PR Radar is updated |
| `hello_timeout` | 4001 | No `hello` within 10 s | Reconnect with backoff |
| `bad_message` | 4002 | Not JSON, no `type`, `hello` not first, invalid field | Reconnect with backoff (and log: it is a client bug) |
| `rate_limited` | 4003 | Over 30 messages per minute | Reconnect with backoff, waiting at least 60 s |
| `replaced` | 4004 | Another connection opened with the same `instanceId` and login; the newest wins | **Do not reconnect**: another process is this radar now |
| `auth_expired` | 4010 | The sign-in code expired before the user approved it | Wait for the user: a *Sign in again* action, or the next radar start |
| `auth_denied` | 4011 | The user clicked *Cancel* on GitHub | Same as `auth_expired` |
| `auth_unavailable` | 4012 | The hub cannot reach GitHub to check identity or membership | Reconnect with backoff, keep the stored token |
| `bad_token` | 4013 | `hubToken` unknown to the hub | Delete the stored token, reconnect **once** right away without it (this starts a sign-in) |
| `not_member` | 4014 | The GitHub account is not an active member of the org | Delete the stored token, **stop** until the radar restarts |
| `revoked` | 4015 | The token was revoked by the hub admin, or the user left the org | Delete the stored token, **stop** until the radar restarts |
| — | 1001 | The hub is restarting | Reconnect with backoff |
| — | 1009 | A message over 16 KiB | Reconnect with backoff (and log) |
| — | 1011 | Hub internal error | Reconnect with backoff |
| — (HTTP 429 on upgrade) | — | Too many connections from this IP | Reconnect with backoff |
| — (network error, 502, 503…) | — | Hub down or unreachable | Reconnect with backoff |

## Reconnection

Exponential backoff with full jitter, capped:

```
delay = random(0, min(300 s, 2 s × 2^attempt))
```

- `attempt` starts at 0 and grows on each failed or dropped connection.
- It goes back to 0 once a connection has stayed up for 60 s after `welcome`.
- `rate_limited` waits at least 60 s.
- **Never reconnect** after `not_member`, `revoked`, `replaced` or
  `protocol_unsupported`; after `auth_expired` or `auth_denied`, only on a user action.
  A radar that loops on a refusal would sign in the user again and again.
- On a clean stop of the radar (Ctrl+C, *Update & restart*), close with 1000 so the hub
  shows it offline at once instead of after the 75 s pong timeout.

## Authentication

1. **First connection on a machine.** No `hubToken`: the hub starts GitHub's
   [device flow](https://docs.github.com/en/apps/oauth-apps/building-oauth-apps/authorizing-oauth-apps#device-flow)
   for the OAuth app *PR Radar* and sends `auth_required` with the code. The app asks
   for **no scope**: the user grants read access to their public profile only. The hub
   reads the login, checks with its own credentials that this login is an active member
   of the `ForestAdmin` organisation, then **throws the GitHub token away**. It issues a
   hub token and returns it in `welcome`.
2. **Every later connection.** `hello` carries the hub token. No GitHub round trip.
3. **Membership is re-checked daily** by the hub. A member who left the organisation
   has their tokens revoked; their connections are closed with `revoked`.
4. **Manual revocation**: `radar-hub revoke <login>` on the hub, same effect.
5. **Exceptions**: `radar-hub invite <login>` issues a hub token by hand, for someone
   outside the organisation. The radar uses it like any stored token.

The hub token looks like `rh_` followed by 43 base64url characters (32 random bytes).
It does not expire; it stops working only when revoked.

### Storing the hub token

The hub token only gives access to the presence list, but it is still a credential:

- store it in a file outside the clone, readable by the user only, e.g.
  `~/.config/pr-radar/hub-token` with mode `600`;
- never print it in the console or the page, never send it to the browser;
- never put it in a URL: it travels inside `hello` only.

## Limits

| Limit | Value |
| --- | --- |
| Message size | 16 KiB |
| `hello` delay | 10 s |
| Messages per connection | 30 per minute, all types |
| Kudos | 1 per minute per sender → recipient pair (extra ignored) |
| Theme offers | 5 per minute per sender (extra ignored) |
| Connections per IP | 64 (one office NAT can carry the whole team) |
| Sign-ins waiting for approval per IP | 5 |
| Radars per login | 10; the 11th closes the oldest with `replaced` |

## What the hub stores

- Per login: avatar URL, SHA-256 hashes of the hub tokens with their issue date and
  revocation, first and last time seen, last version seen.
- Live presence and availability stay in memory and are lost when the hub restarts;
  radars reconnect within seconds and resend their state in `hello`.
- Kudos and theme offers are relayed and forgotten: neither is written to disk or to
  the logs. Only the recent send times are kept in memory, for the limits.
- No GitHub token, no IP address, no pull request data.

## Extending the protocol

Future features (a "new version" notice pushed by the hub, for instance) arrive as **new message types** within protocol 1. A radar that does not know them
ignores them (guarantee 2). Each new type is added to this file before the hub sends it.
