package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// fakeGitHub stands in for github.com and api.github.com: no test touches the network.
type fakeGitHub struct {
	mu       sync.Mutex
	members  map[string]bool // login -> active member
	approve  string          // login granted on the next device approval
	outcome  string          // "ok", "denied", "expired"
	pending  int             // polls answered authorization_pending first
	down     bool
	expiry   string
	calls    map[string]int
	orgToken string
	devices  int
}

func newFakeGitHub() *fakeGitHub {
	return &fakeGitHub{members: map[string]bool{}, outcome: "ok", calls: map[string]int{}, orgToken: "org-secret", expiry: "2027-10-31 23:00:00 UTC"}
}

func (f *fakeGitHub) count(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[name]
}

func (f *fakeGitHub) set(fn func(f *fakeGitHub)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

func (f *fakeGitHub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		http.Error(w, "down", http.StatusServiceUnavailable)
		return
	}
	reply := func(v any) { w.Header().Set("Content-Type", "application/json"); _ = json.NewEncoder(w).Encode(v) }
	orgAuth := func() bool {
		if r.Header.Get("Authorization") != "Bearer "+f.orgToken {
			w.WriteHeader(http.StatusUnauthorized)
			return false
		}
		w.Header().Set("Github-Authentication-Token-Expiration", f.expiry)
		return true
	}
	switch {
	case r.Method == "POST" && r.URL.Path == "/login/device/code":
		f.calls["device"]++
		f.devices++
		reply(map[string]any{"device_code": fmt.Sprintf("dc-%d", f.devices), "user_code": "ABCD-1234", "verification_uri": "https://github.com/login/device", "expires_in": 900, "interval": 0})
	case r.Method == "POST" && r.URL.Path == "/login/oauth/access_token":
		f.calls["poll"]++
		if f.pending > 0 {
			f.pending--
			reply(map[string]string{"error": "authorization_pending"})
			return
		}
		switch f.outcome {
		case "denied":
			reply(map[string]string{"error": "access_denied"})
		case "expired":
			reply(map[string]string{"error": "expired_token"})
		default:
			reply(map[string]string{"access_token": "user-token-" + f.approve, "token_type": "bearer"})
		}
	case r.URL.Path == "/user":
		f.calls["user"]++
		login, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer user-token-")
		if !ok {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		reply(map[string]string{"login": login, "avatar_url": "https://avatars.example/" + login})
	case strings.HasPrefix(r.URL.Path, "/orgs/ForestAdmin/memberships/"):
		f.calls["membership"]++
		if !orgAuth() {
			return
		}
		login := strings.TrimPrefix(r.URL.Path, "/orgs/ForestAdmin/memberships/")
		if !f.members[login] {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		reply(map[string]string{"state": "active", "role": "member"})
	case strings.HasPrefix(r.URL.Path, "/users/"):
		if !orgAuth() {
			return
		}
		login := strings.TrimPrefix(r.URL.Path, "/users/")
		reply(map[string]string{"login": login, "avatar_url": "https://avatars.example/" + login})
	case r.URL.Path == "/rate_limit":
		if !orgAuth() {
			return
		}
		reply(map[string]any{})
	default:
		http.NotFound(w, r)
	}
}

type testHub struct {
	*Hub
	fake  *fakeGitHub
	wsURL string
	srv   *httptest.Server
}

func newTestHub(t *testing.T, tweak ...func(*Config)) *testHub {
	t.Helper()
	fake := newFakeGitHub()
	ghSrv := httptest.NewServer(fake)
	t.Cleanup(ghSrv.Close)

	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "github-token")
	if err := os.WriteFile(tokenFile, []byte(fake.orgToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := defaultConfig()
	cfg.DataDir = filepath.Join(dir, "data")
	cfg.ClientID = "test-client"
	cfg.TokenFile = tokenFile
	cfg.GitHubWebURL, cfg.GitHubAPIURL = ghSrv.URL, ghSrv.URL
	cfg.HelloTimeout = 300 * time.Millisecond
	cfg.PingInterval = 50 * time.Millisecond
	cfg.PongWait = 100 * time.Millisecond
	cfg.PresenceThrottle = 10 * time.Millisecond
	cfg.MinPollInterval = 5 * time.Millisecond
	for _, fn := range tweak {
		fn(&cfg)
	}
	store, err := openStore(cfg.DataDir, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	h := newHub(cfg, store, newGitHub(cfg, time.Now), log.New(io.Discard, "", 0))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go h.broadcaster(ctx)

	srv := httptest.NewServer(h.publicHandler())
	t.Cleanup(srv.Close)
	return &testHub{Hub: h, fake: fake, wsURL: "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws", srv: srv}
}

type client struct {
	t    *testing.T
	c    *websocket.Conn
	msgs chan map[string]any
	err  chan error
}

func (th *testHub) dial(t *testing.T) *client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, th.wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c.SetReadLimit(1 << 20)
	t.Cleanup(func() { c.CloseNow() })
	cl := &client{t: t, c: c, msgs: make(chan map[string]any, 100), err: make(chan error, 1)}
	go func() {
		for {
			_, data, err := c.Read(context.Background())
			if err != nil {
				cl.err <- err
				close(cl.msgs)
				return
			}
			var m map[string]any
			_ = json.Unmarshal(data, &m)
			cl.msgs <- m
		}
	}()
	return cl
}

// dialSilent opens a connection that never reads, so it never answers a ping.
func (th *testHub) dialSilent(t *testing.T) *websocket.Conn {
	t.Helper()
	c, _, err := websocket.Dial(context.Background(), th.wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { c.CloseNow() })
	return c
}

func (cl *client) send(v any) {
	cl.t.Helper()
	data, _ := json.Marshal(v)
	if err := cl.c.Write(context.Background(), websocket.MessageText, data); err != nil {
		cl.t.Fatalf("send: %v", err)
	}
}

func (cl *client) sendRaw(data []byte) {
	_ = cl.c.Write(context.Background(), websocket.MessageText, data)
}

// expect waits for the next message of that type, skipping others.
func (cl *client) expect(typ string) map[string]any {
	cl.t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case m, ok := <-cl.msgs:
			if !ok {
				cl.t.Fatalf("connection closed while waiting for %q", typ)
			}
			if m["type"] == typ {
				return m
			}
		case <-deadline:
			cl.t.Fatalf("no %q message", typ)
		}
	}
}

// presenceWhere waits for a presence message matching pred.
func (cl *client) presenceWhere(pred func(presenceMessage) bool) presenceMessage {
	cl.t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case m, ok := <-cl.msgs:
			if !ok {
				cl.t.Fatal("connection closed while waiting for presence")
			}
			if m["type"] != "presence" {
				continue
			}
			var p presenceMessage
			data, _ := json.Marshal(m)
			_ = json.Unmarshal(data, &p)
			if pred(p) {
				return p
			}
		case <-deadline:
			cl.t.Fatal("no matching presence")
		}
	}
}

// closedWith waits for the hub to close the connection and checks the error code
// and the close code.
func (cl *client) closedWith(code string, status websocket.StatusCode) {
	cl.t.Helper()
	deadline := time.After(3 * time.Second)
	gotError := code == ""
	for {
		select {
		case m, ok := <-cl.msgs:
			if !ok {
				err := <-cl.err
				if got := websocket.CloseStatus(err); got != status {
					cl.t.Fatalf("close status %d, want %d (%v)", got, status, err)
				}
				if !gotError {
					cl.t.Fatalf("no error message with code %q before close", code)
				}
				return
			}
			if m["type"] == "error" {
				if m["code"] != code {
					cl.t.Fatalf("error code %v, want %q", m["code"], code)
				}
				gotError = true
			}
		case <-deadline:
			cl.t.Fatalf("connection not closed (want %q)", code)
		}
	}
}

func hello(instance string, token string) map[string]any {
	m := map[string]any{"type": "hello", "protocol": 1, "instanceId": instance, "tabs": 1,
		"version": map[string]any{"commit": "129a21f3c4d5e6f708192a3b4c5d6e7f80912a3b", "behind": 0}}
	if token != "" {
		m["hubToken"] = token
	}
	return m
}

func (th *testHub) issue(t *testing.T, login string) string {
	t.Helper()
	th.fake.set(func(f *fakeGitHub) { f.members[login] = true })
	token, err := th.store.IssueToken(login, "https://avatars.example/"+login, "github")
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func (th *testHub) online(t *testing.T, login, instance string) *client {
	t.Helper()
	cl := th.dial(t)
	cl.send(hello(instance, th.issue(t, login)))
	cl.expect("welcome")
	return cl
}

// Authentication.

func TestSignInMember(t *testing.T) {
	th := newTestHub(t)
	th.fake.set(func(f *fakeGitHub) { f.members["alice"] = true; f.approve = "alice"; f.pending = 2 })

	cl := th.dial(t)
	cl.send(hello("instance-a1", ""))
	auth := cl.expect("auth_required")
	if auth["userCode"] != "ABCD-1234" || auth["verificationUri"] != "https://github.com/login/device" {
		t.Fatalf("auth_required = %v", auth)
	}
	w := cl.expect("welcome")
	if w["login"] != "alice" || w["protocol"] != float64(1) {
		t.Fatalf("welcome = %v", w)
	}
	token, _ := w["hubToken"].(string)
	if !strings.HasPrefix(token, "rh_") || len(token) != 46 {
		t.Fatalf("hubToken = %q", token)
	}
	p := cl.presenceWhere(func(p presenceMessage) bool { return len(p.Online) == 1 })
	if p.Online[0].Login != "alice" || p.Online[0].Tabs != 1 {
		t.Fatalf("presence = %+v", p)
	}

	data, _ := os.ReadFile(filepath.Join(th.cfg.DataDir, "hub.json"))
	if strings.Contains(string(data), token) || strings.Contains(string(data), "user-token") {
		t.Fatal("a token is stored in clear")
	}
	if !strings.Contains(string(data), hashToken(token)) {
		t.Fatal("token hash not stored")
	}
}

func TestHubTokenReuse(t *testing.T) {
	th := newTestHub(t)
	th.fake.set(func(f *fakeGitHub) { f.members["alice"] = true; f.approve = "alice" })
	first := th.dial(t)
	first.send(hello("instance-a1", ""))
	token := first.expect("welcome")["hubToken"].(string)
	first.c.Close(websocket.StatusNormalClosure, "")

	calls := th.fake.count("device") + th.fake.count("user") + th.fake.count("membership")
	second := th.dial(t)
	second.send(hello("instance-a1", token))
	w := second.expect("welcome")
	if w["login"] != "alice" {
		t.Fatalf("welcome = %v", w)
	}
	if _, ok := w["hubToken"]; ok {
		t.Fatal("hubToken sent again on reuse")
	}
	if now := th.fake.count("device") + th.fake.count("user") + th.fake.count("membership"); now != calls {
		t.Fatalf("GitHub called on token reuse (%d -> %d calls)", calls, now)
	}
}

func TestSignInNotMember(t *testing.T) {
	th := newTestHub(t)
	th.fake.set(func(f *fakeGitHub) { f.approve = "mallory" })
	cl := th.dial(t)
	cl.send(hello("instance-m1", ""))
	cl.expect("auth_required")
	cl.closedWith("not_member", 4014)
	if len(th.store.Snapshot()) != 0 {
		t.Fatal("a non-member was stored")
	}
}

func TestSignInDeniedAndExpired(t *testing.T) {
	for outcome, want := range map[string]struct {
		code   string
		status websocket.StatusCode
	}{"denied": {"auth_denied", 4011}, "expired": {"auth_expired", 4010}} {
		t.Run(outcome, func(t *testing.T) {
			th := newTestHub(t)
			th.fake.set(func(f *fakeGitHub) { f.outcome = outcome })
			cl := th.dial(t)
			cl.send(hello("instance-x1", ""))
			cl.expect("auth_required")
			cl.closedWith(want.code, want.status)
		})
	}
}

func TestSignInGitHubDown(t *testing.T) {
	th := newTestHub(t)
	th.fake.set(func(f *fakeGitHub) { f.down = true })
	cl := th.dial(t)
	cl.send(hello("instance-x1", ""))
	cl.closedWith("auth_unavailable", 4012)
}

func TestBadToken(t *testing.T) {
	th := newTestHub(t)
	cl := th.dial(t)
	cl.send(hello("instance-x1", "rh_not-a-real-token"))
	cl.closedWith("bad_token", 4013)
}

func TestRevokedToken(t *testing.T) {
	th := newTestHub(t)
	token := th.issue(t, "alice")
	cl := th.dial(t)
	cl.send(hello("instance-a1", token))
	cl.expect("welcome")

	if _, err := th.store.Revoke("alice", "admin", true); err != nil {
		t.Fatal(err)
	}
	th.kickLogin("alice", "revoked", "revoked")
	cl.closedWith("revoked", 4015)

	again := th.dial(t)
	again.send(hello("instance-a1", token))
	again.closedWith("revoked", 4015)

	th.fake.set(func(f *fakeGitHub) { f.approve = "alice" })
	signIn := th.dial(t)
	signIn.send(hello("instance-a1", ""))
	signIn.expect("auth_required")
	signIn.closedWith("revoked", 4015)
}

func TestRevalidateRevokesLeaver(t *testing.T) {
	th := newTestHub(t)
	alice := th.online(t, "alice", "instance-a1")
	bob := th.online(t, "bob", "instance-b1")

	th.fake.set(func(f *fakeGitHub) { f.members["alice"] = false })
	th.revalidate(context.Background())

	alice.closedWith("revoked", 4015)
	bob.presenceWhere(func(p presenceMessage) bool { return len(p.Online) == 1 && p.Online[0].Login == "bob" })
	if th.store.GitHubLogins()[0] != "bob" {
		t.Fatal("alice still has an active token")
	}
}

func TestRevalidateKeepsEveryoneWhenGitHubFails(t *testing.T) {
	th := newTestHub(t)
	alice := th.online(t, "alice", "instance-a1")
	th.fake.set(func(f *fakeGitHub) { f.orgToken = "rotated" })
	th.revalidate(context.Background())

	alice.send(map[string]any{"type": "status", "tabs": 4})
	alice.presenceWhere(func(p presenceMessage) bool { return len(p.Online) == 1 && p.Online[0].Tabs == 4 })
	if st := th.Hub.gh.Status(th.cfg.TokenWarning); st.OK || st.Error == "" {
		t.Fatalf("token status = %+v, want a reported problem", st)
	}
}

// Presence.

func TestSeveralRadarsOfOneLogin(t *testing.T) {
	th := newTestHub(t)
	token := th.issue(t, "alice")
	laptop := th.dial(t)
	laptop.send(hello("instance-laptop", token))
	laptop.expect("welcome")
	desk := th.dial(t)
	h := hello("instance-desk1", token)
	h["tabs"] = 2
	desk.send(h)
	desk.expect("welcome")

	p := laptop.presenceWhere(func(p presenceMessage) bool { return len(p.Online) == 1 && len(p.Online[0].Radars) == 2 })
	if p.Online[0].Tabs != 3 {
		t.Fatalf("tabs = %d, want 3", p.Online[0].Tabs)
	}

	desk.c.Close(websocket.StatusNormalClosure, "")
	laptop.presenceWhere(func(p presenceMessage) bool { return len(p.Online) == 1 && len(p.Online[0].Radars) == 1 })
}

func TestDepartureAndRecent(t *testing.T) {
	th := newTestHub(t)
	alice := th.online(t, "alice", "instance-a1")
	bob := th.online(t, "bob", "instance-b1")
	alice.presenceWhere(func(p presenceMessage) bool { return len(p.Online) == 2 })

	bob.c.Close(websocket.StatusNormalClosure, "")
	p := alice.presenceWhere(func(p presenceMessage) bool { return len(p.Online) == 1 })
	if len(p.Recent) != 1 || p.Recent[0].Login != "bob" {
		t.Fatalf("recent = %+v", p.Recent)
	}
}

func TestSameInstanceReplaced(t *testing.T) {
	th := newTestHub(t)
	token := th.issue(t, "alice")
	old := th.dial(t)
	old.send(hello("instance-a1", token))
	old.expect("welcome")
	fresh := th.dial(t)
	fresh.send(hello("instance-a1", token))
	fresh.expect("welcome")

	old.closedWith("replaced", 4004)
	fresh.presenceWhere(func(p presenceMessage) bool { return len(p.Online) == 1 && len(p.Online[0].Radars) == 1 })
}

func TestPongTimeout(t *testing.T) {
	th := newTestHub(t)
	watcher := th.online(t, "alice", "instance-a1")

	silent := th.dialSilent(t)
	data, _ := json.Marshal(hello("instance-b1", th.issue(t, "bob")))
	if err := silent.Write(context.Background(), websocket.MessageText, data); err != nil {
		t.Fatal(err)
	}
	watcher.presenceWhere(func(p presenceMessage) bool { return len(p.Online) == 2 })
	watcher.presenceWhere(func(p presenceMessage) bool { return len(p.Online) == 1 && p.Online[0].Login == "alice" })
}

func TestStatusUpdatesPresence(t *testing.T) {
	th := newTestHub(t)
	alice := th.online(t, "alice", "instance-a1")
	alice.send(map[string]any{"type": "status", "tabs": 5, "version": map[string]any{"commit": "abcdef1", "behind": 3}})
	p := alice.presenceWhere(func(p presenceMessage) bool { return len(p.Online) == 1 && p.Online[0].Tabs == 5 })
	r := p.Online[0].Radars[0]
	if r.Commit == nil || *r.Commit != "abcdef1" || r.Behind == nil || *r.Behind != 3 {
		t.Fatalf("radar = %+v", r)
	}
}

// Robustness.

func TestUnknownTypeIgnored(t *testing.T) {
	th := newTestHub(t)
	alice := th.online(t, "alice", "instance-a1")
	alice.send(map[string]any{"type": "kudos", "to": "bob", "anything": []int{1, 2}})
	alice.send(map[string]any{"type": "status", "tabs": 7})
	alice.presenceWhere(func(p presenceMessage) bool { return len(p.Online) == 1 && p.Online[0].Tabs == 7 })
}

func TestMessageTooBig(t *testing.T) {
	th := newTestHub(t)
	alice := th.online(t, "alice", "instance-a1")
	alice.sendRaw([]byte(`{"type":"status","pad":"` + strings.Repeat("x", 17<<10) + `"}`))
	alice.closedWith("", websocket.StatusMessageTooBig)
}

func TestHelloTimeout(t *testing.T) {
	th := newTestHub(t)
	cl := th.dial(t)
	cl.closedWith("hello_timeout", 4001)
}

func TestFirstMessageMustBeHello(t *testing.T) {
	th := newTestHub(t)
	cl := th.dial(t)
	cl.send(map[string]any{"type": "status", "tabs": 1})
	cl.closedWith("bad_message", 4002)

	junk := th.dial(t)
	junk.sendRaw([]byte("not json"))
	junk.closedWith("bad_message", 4002)

	badID := th.dial(t)
	badID.send(hello("x", ""))
	badID.closedWith("bad_message", 4002)
}

func TestProtocolUnsupported(t *testing.T) {
	th := newTestHub(t)
	cl := th.dial(t)
	h := hello("instance-a1", "")
	h["protocol"] = 2
	cl.send(h)
	cl.closedWith("protocol_unsupported", 4000)
}

func TestRateLimited(t *testing.T) {
	th := newTestHub(t, func(c *Config) { c.MaxMessagesPerMinute = 5 })
	alice := th.online(t, "alice", "instance-a1")
	for i := range 6 {
		alice.send(map[string]any{"type": "status", "tabs": i})
	}
	alice.closedWith("rate_limited", 4003)
}

func TestConnectionsPerIP(t *testing.T) {
	th := newTestHub(t, func(c *Config) { c.MaxPerIP = 2 })
	th.dial(t)
	th.dial(t)
	_, resp, err := websocket.Dial(context.Background(), th.wsURL, nil)
	if err == nil || resp == nil || resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("third connection: err=%v resp=%v, want HTTP 429", err, resp)
	}
}

func TestClientIPBehindProxy(t *testing.T) {
	r := httptest.NewRequest("GET", "/ws", nil)
	r.RemoteAddr = "127.0.0.1:5555"
	r.Header.Set("X-Forwarded-For", "6.6.6.6, 203.0.113.9")
	if got := clientIP(r); got != "203.0.113.9" {
		t.Fatalf("clientIP = %q", got)
	}
	r.RemoteAddr = "198.51.100.1:5555"
	if got := clientIP(r); got != "198.51.100.1" {
		t.Fatalf("clientIP without proxy = %q", got)
	}
}

// Health and admin.

func TestHealthzAndTokenExpiry(t *testing.T) {
	th := newTestHub(t)
	get := func() map[string]bool {
		resp, err := http.Get(th.srv.URL + "/healthz")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var m map[string]bool
		_ = json.NewDecoder(resp.Body).Decode(&m)
		return m
	}
	if m := get(); !m["ok"] || m["tokenOk"] {
		t.Fatalf("before any check: %v (token not checked yet must not look ok)", m)
	}
	if err := th.Hub.gh.CheckToken(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m := get(); !m["tokenOk"] {
		t.Fatalf("far expiry: %v", m)
	}
	soon := time.Now().Add(10 * 24 * time.Hour).UTC().Format("2006-01-02 15:04:05 UTC")
	th.fake.set(func(f *fakeGitHub) { f.expiry = soon })
	_ = th.Hub.gh.CheckToken(context.Background())
	if m := get(); m["tokenOk"] {
		t.Fatalf("expiry in 10 days: %v", m)
	}
}

func TestAdminRevokeAndInvite(t *testing.T) {
	th := newTestHub(t)
	alice := th.online(t, "alice", "instance-a1")
	admin := httptest.NewServer(th.adminHandler())
	t.Cleanup(admin.Close)

	resp, err := http.Post(admin.URL+"/revoke?login=alice", "", nil)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("revoke: %v %v", err, resp)
	}
	alice.closedWith("revoked", 4015)

	resp, err = http.Post(admin.URL+"/invite?login=alice", "", nil)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("invite: %v %v", err, resp)
	}
	var out map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&out)
	back := th.dial(t)
	back.send(hello("instance-a1", out["hubToken"]))
	back.expect("welcome")
}
