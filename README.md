<p align="center"><img src="assets/logo.png" width="96" alt=""></p>

# PR Radar Hub

A small server that lets every [PR Radar](https://github.com/matthv/pr-radar) of the team
know **who else has their radar running**: how many tabs, on which version, and when they
were last seen.

Each radar opens one WebSocket to the hub. The hub keeps the live list in memory and sends
it to every radar whenever it changes. The protocol, written for whoever implements the
client, is in **[PROTOCOL.md](PROTOCOL.md)**.

## What goes through the hub, and what never does

| Goes through | Never |
| --- | --- |
| GitHub login and avatar URL | GitHub tokens of the users |
| An anonymous id per machine | Pull requests, repositories, titles |
| PR Radar's commit and how far behind `main` it is | IP addresses (not stored, not logged) |
| Number of open tabs | |

**Signing in** uses GitHub's device flow with an OAuth app that asks for **no scope**: the
user approves read access to their public profile, once per machine. The hub reads the
login, checks with its own `read:org` token that the login is an active member of the
organisation, forgets the GitHub token and hands the radar a hub token. Only the SHA-256 of
that hub token is stored.

Membership is checked again every day: someone who left the organisation is revoked on
their own, without anyone thinking of it.

## Commands

```
radar-hub serve              run the hub
radar-hub status [-json]     who is online, versions, tokens (asks the running hub)
radar-hub revoke <login>     revoke a login and refuse its next sign-in
radar-hub unrevoke <login>   allow a revoked login to sign in again
radar-hub invite <login>     issue a hub token by hand (someone outside the org)
radar-hub watch [-tabs N]    act as a radar and print what it receives
```

`status`, `revoke`, `unrevoke` and `invite` talk to the running hub through a unix socket
in its data directory, so they must run as the hub's user with the same `HUB_DATA_DIR`.

`watch` is the test radar: it signs in (showing the GitHub code the first time), reports a
tab count and prints every presence update, kudos and theme offer it receives (a theme
offer is summed up, never printed). Lines typed on its input:

- a number: the new tab count;
- `available`, `focus` or `none`: the new availability;
- `kudos <login>`: send a kudos;
- `theme <login>`: offer a small test theme.

It never takes a token on the command line: it reads `RADAR_HUB_TOKEN` from the
environment, or the token it saved itself in `~/.config/radar-hub/watch-token`.

```
radar-hub watch                                  # wss://radar-hub.matthv.com/ws
HUB_URL=ws://127.0.0.1:8795/ws radar-hub watch   # a local hub
```

## Configuration

Environment variables, see [.env.example](.env.example):

| Variable | Default | |
| --- | --- | --- |
| `HUB_PORT` | `8795` | Always bound to `127.0.0.1`; the reverse proxy faces the internet |
| `HUB_ORG` | `ForestAdmin` | Organisation whose active members may sign in |
| `HUB_GITHUB_CLIENT_ID` | — | OAuth app client ID, **required** for `serve` |
| `HUB_GITHUB_TOKEN_FILE` | `~/.config/radar-hub/github-token` | Classic token, `read:org` only |
| `HUB_DATA_DIR` | `data/` next to the binary | `hub.json` and the admin socket |

### The GitHub side

1. **OAuth app** (*Settings → Developer settings → OAuth Apps*): any homepage and callback
   URL, **Enable Device Flow** checked, no client secret generated.
2. **Classic token** with the single `read:org` scope, from an active member of the
   organisation (authorise it for SSO if the organisation enforces it). Write it to the
   token file without it reaching the shell history:

   ```
   read -rsp 'Token: ' t && printf '%s' "$t" > ~/.config/radar-hub/github-token && chmod 600 ~/.config/radar-hub/github-token && unset t
   ```

   The hub reads the file on every call: replacing it needs no restart.

### When the `read:org` token expires

The hub reads the expiry date GitHub returns with every call. `radar-hub status` shows it,
and `/healthz` answers `"tokenOk": false` from 30 days before, which a monitor can watch.
If it expires anyway, nobody is thrown out: radars already signed in keep working, only new
sign-ins wait (`auth_unavailable`) until a new token is in the file.

## Deploying

Requirements: Go 1.23+ to build, a reverse proxy with TLS and WebSocket upgrade.

```
go build -o radar-hub .
```

- **Service**: [deploy/radar-hub.service](deploy/radar-hub.service), with `Restart=always`
  so that `tools/deploy.sh` can restart it with a plain `kill`, without root. Logs:
  `journalctl -u radar-hub`.
- **Reverse proxy**: [deploy/apache.conf](deploy/apache.conf) for Apache 2.4.47+, where
  `mod_proxy_http` handles the upgrade (`upgrade=websocket`). The hub reads the client IP
  from the last `X-Forwarded-For` entry when the connection comes from loopback, to apply
  its per-IP limits.
- **Health**: `GET /healthz` → `{"ok":true,"tokenOk":true}`.
- **Updating**: `tools/deploy.sh` vets, tests, swaps the binary, then restarts the service.
  Radars reconnect by themselves within seconds.

### Backup

`hub.json` holds the members, token hashes and last-seen dates. Losing it costs one GitHub
sign-in per machine, nothing more; a copy with the server's usual backups is enough. It
contains no secret that could be replayed: only hashes.

## Development

```
go vet ./...
go test -race ./...
```

The tests run a fake GitHub (device flow, user, membership, token expiry) and real
WebSocket connections; nothing reaches the network. They cover sign-in (member, not a
member, cancelled, expired, GitHub down), token reuse and revocation, the daily
re-check, several radars per login, replacement, pong timeout, rate and size limits, an
unknown message type being ignored, and the per-IP limit.
