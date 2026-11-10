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
	// Runs first: the sessions must be over before the temporary data directory goes,
	// since a departing radar still writes to the store.
	t.Cleanup(func() {
		h.closeAll(websocket.StatusGoingAway, "test over")
		done := make(chan struct{})
		go func() { h.sessions.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("sessions still running after the test")
		}
	})
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
	alice.send(map[string]any{"type": "reviewing", "to": "bob", "anything": []int{1, 2}})
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

// Availability, kudos, theme offers.

// expectNo checks that no message of that type arrives within a short delay.
func (cl *client) expectNo(typ string, wait time.Duration) {
	cl.t.Helper()
	deadline := time.After(wait)
	for {
		select {
		case m, ok := <-cl.msgs:
			if !ok {
				cl.t.Fatalf("connection closed while checking for no %q", typ)
			}
			if m["type"] == typ {
				cl.t.Fatalf("unexpected %q: %v", typ, m)
			}
		case <-deadline:
			return
		}
	}
}

func availabilityOf(p presenceMessage, login string) (string, bool) {
	for _, o := range p.Online {
		if o.Login == login {
			if o.Availability == nil {
				return "", true
			}
			return *o.Availability, true
		}
	}
	return "", false
}

func TestAvailability(t *testing.T) {
	th := newTestHub(t)
	token := th.issue(t, "alice")
	laptop := th.dial(t)
	h := hello("instance-laptop", token)
	h["availability"] = "focus"
	laptop.send(h)
	laptop.expect("welcome")
	laptop.presenceWhere(func(p presenceMessage) bool { a, _ := availabilityOf(p, "alice"); return a == "focus" })

	// A second radar without the field keeps the login's value; one that sets it wins.
	desk := th.dial(t)
	desk.send(hello("instance-desk1", token))
	desk.expect("welcome")
	desk.presenceWhere(func(p presenceMessage) bool {
		a, _ := availabilityOf(p, "alice")
		return len(p.Online) == 1 && len(p.Online[0].Radars) == 2 && a == "focus"
	})
	desk.send(map[string]any{"type": "status", "availability": "available"})
	laptop.presenceWhere(func(p presenceMessage) bool { a, _ := availabilityOf(p, "alice"); return a == "available" })

	// Unknown values count as null, without closing.
	laptop.send(map[string]any{"type": "status", "availability": "lunch"})
	laptop.presenceWhere(func(p presenceMessage) bool { a, ok := availabilityOf(p, "alice"); return ok && a == "" })
	laptop.send(map[string]any{"type": "status", "availability": "focus"})
	laptop.presenceWhere(func(p presenceMessage) bool { a, _ := availabilityOf(p, "alice"); return a == "focus" })
	laptop.send(map[string]any{"type": "status", "availability": nil})
	laptop.presenceWhere(func(p presenceMessage) bool { a, ok := availabilityOf(p, "alice"); return ok && a == "" })

	// Status sees it.
	laptop.send(map[string]any{"type": "status", "availability": "focus"})
	laptop.presenceWhere(func(p presenceMessage) bool { a, _ := availabilityOf(p, "alice"); return a == "focus" })
	for _, m := range th.report().Members {
		if m.Login == "alice" && (m.Availability == nil || *m.Availability != "focus") {
			t.Fatalf("report availability = %v", m.Availability)
		}
	}

	// It does not outlive the login's last radar.
	laptop.c.Close(websocket.StatusNormalClosure, "")
	desk.c.Close(websocket.StatusNormalClosure, "")
	again := th.dial(t)
	again.send(hello("instance-laptop", token))
	again.expect("welcome")
	again.presenceWhere(func(p presenceMessage) bool { a, ok := availabilityOf(p, "alice"); return ok && a == "" })
}

func TestKudos(t *testing.T) {
	th := newTestHub(t)
	alice := th.online(t, "alice", "instance-a1")
	bobToken := th.issue(t, "bob")
	bob1 := th.dial(t)
	bob1.send(hello("instance-b1", bobToken))
	bob1.expect("welcome")
	bob2 := th.dial(t)
	bob2.send(hello("instance-b2", bobToken))
	bob2.expect("welcome")
	carol := th.online(t, "carol", "instance-c1")

	alice.send(map[string]any{"type": "kudos", "to": "Bob"})
	for _, b := range []*client{bob1, bob2} {
		k := b.expect("kudos")
		if k["from"] != "alice" || k["avatarUrl"] != "https://avatars.example/alice" || k["at"] == "" {
			t.Fatalf("kudos = %v", k)
		}
	}
	carol.expectNo("kudos", 100*time.Millisecond)

	// Second kudos to the same person within the minute: ignored, connection kept.
	alice.send(map[string]any{"type": "kudos", "to": "bob"})
	bob1.expectNo("kudos", 150*time.Millisecond)
	// Another pair is not limited.
	carol.send(map[string]any{"type": "kudos", "to": "bob"})
	if k := bob1.expect("kudos"); k["from"] != "carol" {
		t.Fatalf("kudos = %v", k)
	}

	// The window passes.
	th.mu.Lock()
	for k := range th.kudosSent {
		th.kudosSent[k] = time.Now().Add(-61 * time.Second)
	}
	th.mu.Unlock()
	alice.send(map[string]any{"type": "kudos", "to": "bob"})
	bob1.expect("kudos")

	// To oneself: ignored.
	alice.send(map[string]any{"type": "kudos", "to": "alice"})
	alice.expectNo("kudos", 100*time.Millisecond)

	// Offline recipient.
	alice.send(map[string]any{"type": "kudos", "to": "dave"})
	u := alice.expect("undelivered")
	if u["kind"] != "kudos" || u["to"] != "dave" {
		t.Fatalf("undelivered = %v", u)
	}

	// Still connected after all that.
	alice.send(map[string]any{"type": "status", "tabs": 4})
	alice.presenceWhere(func(p presenceMessage) bool { return p.Online[0].Tabs == 4 })

	// Malformed recipient is a bad message.
	alice.send(map[string]any{"type": "kudos", "to": "not a login"})
	alice.closedWith("bad_message", 4002)
}

func theme(name string) map[string]any {
	return map[string]any{"name": name, "label": "Forêt", "icon": "🌲", "css": "body{background:#0b3}", "js": "console.log('secret-theme-code')"}
}

func TestThemeOffer(t *testing.T) {
	var logs strings.Builder
	var logMu sync.Mutex
	th := newTestHub(t)
	th.log = log.New(writerFunc(func(p []byte) (int, error) { logMu.Lock(); defer logMu.Unlock(); return logs.Write(p) }), "", 0)

	alice := th.online(t, "alice", "instance-a1")
	bob := th.online(t, "bob", "instance-b1")

	alice.send(map[string]any{"type": "theme_offer", "to": "bob", "theme": theme("foret")})
	o := bob.expect("theme_offer")
	id, _ := o["id"].(string)
	th1, _ := o["theme"].(map[string]any)
	if o["from"] != "alice" || len(id) < 8 || th1["name"] != "foret" || th1["js"] != "console.log('secret-theme-code')" {
		t.Fatalf("offer = %v", o)
	}
	// CSS only.
	css := theme("css-only")
	css["js"] = nil
	alice.send(map[string]any{"type": "theme_offer", "to": "bob", "theme": css})
	if o2 := bob.expect("theme_offer"); o2["id"] == id {
		t.Fatal("offer ids must differ")
	}

	// Five per minute per sender; the rest is ignored without closing.
	for range 4 {
		alice.send(map[string]any{"type": "theme_offer", "to": "bob", "theme": theme("foret")})
	}
	for range 3 {
		bob.expect("theme_offer")
	}
	bob.expectNo("theme_offer", 150*time.Millisecond)

	// Offline recipient.
	bob.send(map[string]any{"type": "theme_offer", "to": "carol", "theme": theme("x")})
	if u := bob.expect("undelivered"); u["kind"] != "theme_offer" || u["to"] != "carol" {
		t.Fatalf("undelivered = %v", u)
	}

	// Nothing of the theme on disk or in the logs.
	data, err := os.ReadFile(filepath.Join(th.cfg.DataDir, "hub.json"))
	if err != nil {
		entries, _ := os.ReadDir(th.cfg.DataDir)
		for _, e := range entries {
			b, _ := os.ReadFile(filepath.Join(th.cfg.DataDir, e.Name()))
			data = append(data, b...)
		}
	}
	logMu.Lock()
	all := string(data) + logs.String()
	logMu.Unlock()
	if strings.Contains(all, "secret-theme-code") || strings.Contains(all, "background:#0b3") {
		t.Fatal("theme content leaked to disk or logs")
	}
	alice.send(map[string]any{"type": "status", "tabs": 2})
	alice.presenceWhere(func(p presenceMessage) bool { return p.Online[0].Tabs == 2 })
}

func TestThemeOfferShape(t *testing.T) {
	bad := []any{
		"not an object",
		nil,
		map[string]any{"name": "Bad Name", "label": "x", "icon": "x", "css": ""},
		map[string]any{"name": "ok", "icon": "x", "css": ""},
		map[string]any{"name": "ok", "label": 3, "icon": "x", "css": ""},
		map[string]any{"name": "ok", "label": "x", "icon": "x"},
		map[string]any{"name": "ok", "label": "x", "icon": "x", "css": "", "js": 4},
		map[string]any{"name": "ok", "label": strings.Repeat("l", 61), "icon": "x", "css": ""},
	}
	for i, b := range bad {
		th := newTestHub(t)
		alice := th.online(t, "alice", "instance-a1")
		th.online(t, "bob", "instance-b1")
		alice.send(map[string]any{"type": "theme_offer", "to": "bob", "theme": b})
		t.Run(fmt.Sprint(i), func(t *testing.T) { alice.t = t; alice.closedWith("bad_message", 4002) })
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// Wizz.

func acceptsWizzOf(p presenceMessage, login string) (bool, bool) {
	for _, o := range p.Online {
		if o.Login == login {
			return o.AcceptsWizz, true
		}
	}
	return false, false
}

func TestWizz(t *testing.T) {
	var logs strings.Builder
	var logMu sync.Mutex
	th := newTestHub(t)
	th.log = log.New(writerFunc(func(p []byte) (int, error) { logMu.Lock(); defer logMu.Unlock(); return logs.Write(p) }), "", 0)

	alice := th.online(t, "alice", "instance-a1")
	bobToken := th.issue(t, "bob")
	bob1 := th.dial(t)
	bob1.send(hello("instance-b1", bobToken))
	bob1.expect("welcome")
	bob2 := th.dial(t)
	bob2.send(hello("instance-b2", bobToken))
	bob2.expect("welcome")
	carol := th.online(t, "carol", "instance-c1")
	ref1 := strings.Repeat("ab", 32)
	ref2 := strings.Repeat("cd", 32)

	// Plain wizz to every radar of the recipient, no ref field.
	alice.send(map[string]any{"type": "wizz", "to": "BOB"})
	for _, b := range []*client{bob1, bob2} {
		z := b.expect("wizz")
		if _, has := z["ref"]; has || z["from"] != "alice" || z["avatarUrl"] != "https://avatars.example/alice" || z["at"] == "" {
			t.Fatalf("wizz = %v", z)
		}
	}
	carol.expectNo("wizz", 100*time.Millisecond)
	// A second plain one within 5 minutes: ignored.
	alice.send(map[string]any{"type": "wizz", "to": "bob", "ref": nil})
	bob1.expectNo("wizz", 150*time.Millisecond)

	// A nudge on a PR has its own limit, per ref.
	alice.send(map[string]any{"type": "wizz", "to": "bob", "ref": ref1})
	if z := bob1.expect("wizz"); z["ref"] != ref1 {
		t.Fatalf("wizz = %v", z)
	}
	alice.send(map[string]any{"type": "wizz", "to": "bob", "ref": ref1})
	bob1.expectNo("wizz", 150*time.Millisecond)
	alice.send(map[string]any{"type": "wizz", "to": "bob", "ref": ref2})
	if z := bob1.expect("wizz"); z["ref"] != ref2 {
		t.Fatalf("wizz = %v", z)
	}

	// Quiet periods end: the plain one after 5 minutes, the ref one after 10.
	age := func(d time.Duration) {
		th.mu.Lock()
		for k, until := range th.wizzUntil {
			th.wizzUntil[k] = until.Add(-d)
		}
		th.mu.Unlock()
	}
	age(4*time.Minute + 50*time.Second)
	alice.send(map[string]any{"type": "wizz", "to": "bob"})
	bob1.expectNo("wizz", 150*time.Millisecond)
	age(11 * time.Second)
	alice.send(map[string]any{"type": "wizz", "to": "bob", "ref": ref1})
	bob1.expectNo("wizz", 150*time.Millisecond)
	alice.send(map[string]any{"type": "wizz", "to": "bob"})
	bob1.expect("wizz")
	age(5 * time.Minute)
	alice.send(map[string]any{"type": "wizz", "to": "bob", "ref": ref1})
	bob1.expect("wizz")

	// Bob refuses plain wizzes: presence says so, plain ones are dropped, nudges pass.
	bob1.send(map[string]any{"type": "status", "acceptsWizz": false})
	carol.presenceWhere(func(p presenceMessage) bool { a, ok := acceptsWizzOf(p, "bob"); return ok && !a })
	carol.send(map[string]any{"type": "wizz", "to": "bob"})
	bob1.expectNo("wizz", 150*time.Millisecond)
	carol.send(map[string]any{"type": "wizz", "to": "bob", "ref": ref1})
	if z := bob1.expect("wizz"); z["from"] != "carol" || z["ref"] != ref1 {
		t.Fatalf("wizz = %v", z)
	}
	// The dropped plain wizz did not use carol's quiet period.
	bob2.send(map[string]any{"type": "status", "acceptsWizz": true})
	carol.presenceWhere(func(p presenceMessage) bool { a, _ := acceptsWizzOf(p, "bob"); return a })
	carol.send(map[string]any{"type": "wizz", "to": "bob"})
	bob1.expect("wizz")
	// Non-boolean values keep the current one.
	bob1.send(map[string]any{"type": "status", "acceptsWizz": "no", "tabs": 9})
	p := carol.presenceWhere(func(p presenceMessage) bool {
		for _, o := range p.Online {
			if o.Login == "bob" && o.Tabs == 10 {
				return true
			}
		}
		return false
	})
	if a, _ := acceptsWizzOf(p, "bob"); !a {
		t.Fatal("acceptsWizz changed by a non-boolean")
	}

	// To oneself: ignored. Offline: undelivered.
	alice.send(map[string]any{"type": "wizz", "to": "alice"})
	alice.expectNo("wizz", 100*time.Millisecond)
	alice.send(map[string]any{"type": "wizz", "to": "dave", "ref": ref1})
	if u := alice.expect("undelivered"); u["kind"] != "wizz" || u["to"] != "dave" {
		t.Fatalf("undelivered = %v", u)
	}

	logMu.Lock()
	leaked := strings.Contains(logs.String(), ref1) || strings.Contains(logs.String(), ref2)
	logMu.Unlock()
	if leaked {
		t.Fatal("ref in the logs")
	}
}

func TestAcceptsWizzBelongsToLogin(t *testing.T) {
	th := newTestHub(t)
	watcher := th.online(t, "carol", "instance-c1")
	token := th.issue(t, "bob")
	b1 := th.dial(t)
	h := hello("instance-b1", token)
	h["acceptsWizz"] = false
	b1.send(h)
	b1.expect("welcome")
	watcher.presenceWhere(func(p presenceMessage) bool { a, ok := acceptsWizzOf(p, "bob"); return ok && !a })
	for _, m := range th.report().Members {
		if m.Login == "bob" && (m.AcceptsWizz == nil || *m.AcceptsWizz) {
			t.Fatalf("report acceptsWizz = %v", m.AcceptsWizz)
		}
	}
	// A second radar without the field keeps it; the default comes back once offline.
	b2 := th.dial(t)
	b2.send(hello("instance-b2", token))
	b2.expect("welcome")
	watcher.presenceWhere(func(p presenceMessage) bool {
		a, _ := acceptsWizzOf(p, "bob")
		for _, o := range p.Online {
			if o.Login == "bob" {
				return len(o.Radars) == 2 && !a
			}
		}
		return false
	})
	b1.c.Close(websocket.StatusNormalClosure, "")
	b2.c.Close(websocket.StatusNormalClosure, "")
	watcher.presenceWhere(func(p presenceMessage) bool { return len(p.Online) == 1 })
	again := th.dial(t)
	again.send(hello("instance-b1", token))
	again.expect("welcome")
	watcher.presenceWhere(func(p presenceMessage) bool { a, ok := acceptsWizzOf(p, "bob"); return ok && a })
}

func TestWizzBadRef(t *testing.T) {
	for i, ref := range []any{"ABCD", strings.Repeat("A", 64), strings.Repeat("a", 63), strings.Repeat("g", 64), 42} {
		th := newTestHub(t)
		alice := th.online(t, "alice", "instance-a1")
		th.online(t, "bob", "instance-b1")
		alice.send(map[string]any{"type": "wizz", "to": "bob", "ref": ref})
		t.Run(fmt.Sprint(i), func(t *testing.T) { alice.t = t; alice.closedWith("bad_message", 4002) })
	}
	th := newTestHub(t)
	alice := th.online(t, "alice", "instance-a1")
	alice.send(map[string]any{"type": "wizz", "to": "not a login"})
	alice.closedWith("bad_message", 4002)
}

func TestNudgeCapPerPair(t *testing.T) {
	th := newTestHub(t)
	alice := th.online(t, "alice", "instance-a1")
	bob := th.online(t, "bob", "instance-b1")
	carol := th.online(t, "carol", "instance-c1")
	ref := func(i int) string { return fmt.Sprintf("%064x", i) }

	for i := 1; i <= 3; i++ {
		alice.send(map[string]any{"type": "wizz", "to": "bob", "ref": ref(i)})
		if z := bob.expect("wizz"); z["ref"] != ref(i) {
			t.Fatalf("wizz %d = %v", i, z)
		}
	}
	// A 4th PR within 10 minutes: dropped, connection kept.
	alice.send(map[string]any{"type": "wizz", "to": "bob", "ref": ref(4)})
	bob.expectNo("wizz", 150*time.Millisecond)
	// Plain wizzes and other pairs are not affected.
	alice.send(map[string]any{"type": "wizz", "to": "bob"})
	if z := bob.expect("wizz"); z["ref"] != nil {
		t.Fatalf("wizz = %v", z)
	}
	alice.send(map[string]any{"type": "wizz", "to": "carol", "ref": ref(5)})
	carol.expect("wizz")
	carol.send(map[string]any{"type": "wizz", "to": "bob", "ref": ref(6)})
	bob.expect("wizz")

	age := func(d time.Duration) {
		th.mu.Lock()
		for k, times := range th.nudgesSent {
			for i := range times {
				times[i] = times[i].Add(-d)
			}
			th.nudgesSent[k] = times
		}
		for k, until := range th.wizzUntil {
			th.wizzUntil[k] = until.Add(-d)
		}
		th.mu.Unlock()
	}
	// Still capped just before 10 minutes; the dropped one did not count.
	age(9*time.Minute + 50*time.Second)
	alice.send(map[string]any{"type": "wizz", "to": "bob", "ref": ref(4)})
	bob.expectNo("wizz", 150*time.Millisecond)
	age(11 * time.Second)
	alice.send(map[string]any{"type": "wizz", "to": "bob", "ref": ref(4)})
	if z := bob.expect("wizz"); z["ref"] != ref(4) {
		t.Fatalf("wizz = %v", z)
	}
}

// Release tag.

func TestVersionTag(t *testing.T) {
	th := newTestHub(t)
	token := th.issue(t, "alice")
	cl := th.dial(t)
	h := hello("instance-a1", token)
	h["version"] = map[string]any{"commit": "abcdef1", "behind": 0, "tag": "v1.2.0"}
	cl.send(h)
	cl.expect("welcome")
	tagIs := func(want string) func(presenceMessage) bool {
		return func(p presenceMessage) bool {
			if len(p.Online) != 1 || len(p.Online[0].Radars) != 1 {
				return false
			}
			got := p.Online[0].Radars[0].Tag
			return (want == "" && got == nil) || (got != nil && *got == want)
		}
	}
	cl.presenceWhere(tagIs("v1.2.0"))

	// A change triggers a presence; a version without tag clears it.
	cl.send(map[string]any{"type": "status", "version": map[string]any{"commit": "abcdef1", "behind": 0, "tag": "v1.10.3"}})
	cl.presenceWhere(tagIs("v1.10.3"))
	cl.send(map[string]any{"type": "status", "version": map[string]any{"commit": "abcdef1", "behind": 0}})
	cl.presenceWhere(tagIs(""))

	// Odd values count as null and never close the connection.
	for _, bad := range []any{"1.2.0", "v1.2", "v1.2.0-beta.1", "V1.2.0", " v1.2.0", 12, true, map[string]any{}, []any{"v1.2.0"}, "v١.٢.٣"} {
		cl.send(map[string]any{"type": "status", "version": map[string]any{"commit": "abcdef1", "behind": 0, "tag": "v9.9.9"}})
		cl.presenceWhere(tagIs("v9.9.9"))
		cl.send(map[string]any{"type": "status", "version": map[string]any{"commit": "abcdef1", "behind": 0, "tag": bad}})
		cl.presenceWhere(tagIs(""))
	}

	// The JSON always carries the field.
	cl.send(map[string]any{"type": "status", "tabs": 2})
	for {
		m := cl.expect("presence")
		online := m["online"].([]any)
		r := online[0].(map[string]any)["radars"].([]any)[0].(map[string]any)
		if r["tabs"].(float64) != 2 {
			continue
		}
		if _, has := r["tag"]; !has {
			t.Fatal("radars[].tag missing")
		}
		break
	}

	// Status shows it.
	cl.send(map[string]any{"type": "status", "version": map[string]any{"commit": "abcdef1", "behind": 0, "tag": "v2.0.0"}})
	cl.presenceWhere(tagIs("v2.0.0"))
	r := th.report()
	if len(r.Members) != 1 || len(r.Members[0].Radars) != 1 || r.Members[0].Radars[0].Tag == nil || *r.Members[0].Radars[0].Tag != "v2.0.0" {
		t.Fatalf("report = %+v", r.Members)
	}
}
