package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

const protocolVersion = 1

var closeCodes = map[string]websocket.StatusCode{
	"protocol_unsupported": 4000,
	"hello_timeout":        4001,
	"bad_message":          4002,
	"rate_limited":         4003,
	"replaced":             4004,
	"auth_expired":         4010,
	"auth_denied":          4011,
	"auth_unavailable":     4012,
	"bad_token":            4013,
	"not_member":           4014,
	"revoked":              4015,
}

var (
	instanceIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{8,64}$`)
	commitPattern     = regexp.MustCompile(`^[0-9a-f]{4,64}$`)
)

type Hub struct {
	cfg   Config
	store *Store
	gh    *GitHub
	now   func() time.Time
	log   *log.Logger

	mu      sync.Mutex
	radars  map[string][]*radar
	perIP   map[string]int
	pending map[string]int
	dirty   bool
}

type radar struct {
	conn       *websocket.Conn
	login      string
	avatarURL  string
	instanceID string
	version    Version
	tabs       int
	since      time.Time
	notify     chan struct{}
	kickOnce   sync.Once
}

func newHub(cfg Config, store *Store, gh *GitHub, logger *log.Logger) *Hub {
	return &Hub{
		cfg:     cfg,
		store:   store,
		gh:      gh,
		now:     time.Now,
		log:     logger,
		radars:  map[string][]*radar{},
		perIP:   map[string]int{},
		pending: map[string]int{},
	}
}

// Messages.

type inMessage struct {
	Type       string   `json:"type"`
	Protocol   *int     `json:"protocol"`
	HubToken   *string  `json:"hubToken"`
	InstanceID string   `json:"instanceId"`
	Version    *Version `json:"version"`
	Tabs       *int     `json:"tabs"`
}

func (m *inMessage) validate() bool {
	if m.Tabs != nil && (*m.Tabs < 0 || *m.Tabs > 1000) {
		return false
	}
	if m.Version != nil {
		if c := m.Version.Commit; c != nil && !commitPattern.MatchString(*c) {
			return false
		}
		if b := m.Version.Behind; b != nil && (*b < 0 || *b > 1_000_000) {
			return false
		}
	}
	return true
}

type errorMessage struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

type authRequiredMessage struct {
	Type            string `json:"type"`
	UserCode        string `json:"userCode"`
	VerificationURI string `json:"verificationUri"`
	ExpiresIn       int    `json:"expiresIn"`
}

type welcomeMessage struct {
	Type      string `json:"type"`
	Protocol  int    `json:"protocol"`
	Login     string `json:"login"`
	AvatarURL string `json:"avatarUrl"`
	HubToken  string `json:"hubToken,omitempty"`
}

type presenceRadar struct {
	Commit *string `json:"commit"`
	Behind *int    `json:"behind"`
	Tabs   int     `json:"tabs"`
	Since  string  `json:"since"`
}

type presenceOnline struct {
	Login     string          `json:"login"`
	AvatarURL string          `json:"avatarUrl"`
	Since     string          `json:"since"`
	Tabs      int             `json:"tabs"`
	Radars    []presenceRadar `json:"radars"`
}

type presenceRecent struct {
	Login     string `json:"login"`
	AvatarURL string `json:"avatarUrl"`
	LastSeen  string `json:"lastSeen"`
}

type presenceMessage struct {
	Type   string           `json:"type"`
	Online []presenceOnline `json:"online"`
	Recent []presenceRecent `json:"recent"`
}

func stamp(t time.Time) string { return t.UTC().Truncate(time.Second).Format(time.RFC3339) }

// Connection bookkeeping.

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	// Behind the reverse proxy every connection comes from loopback; the proxy appends
	// the real client address last.
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			parts := strings.Split(xff, ",")
			return strings.TrimSpace(parts[len(parts)-1])
		}
	}
	return host
}

func (h *Hub) acquireIP(ip string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.perIP[ip] >= h.cfg.MaxPerIP {
		return false
	}
	h.perIP[ip]++
	return true
}

func (h *Hub) releaseIP(ip string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.perIP[ip]--; h.perIP[ip] <= 0 {
		delete(h.perIP, ip)
	}
}

func (h *Hub) acquirePending(ip string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.pending[ip] >= h.cfg.MaxPendingPerIP {
		return false
	}
	h.pending[ip]++
	return true
}

func (h *Hub) releasePending(ip string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.pending[ip]--; h.pending[ip] <= 0 {
		delete(h.pending, ip)
	}
}

func (h *Hub) register(r *radar) {
	h.mu.Lock()
	defer h.mu.Unlock()
	k := key(r.login)
	list := h.radars[k][:0:0]
	for _, old := range h.radars[k] {
		if old.instanceID == r.instanceID {
			old.kick("replaced", "another connection opened for this radar")
			continue
		}
		list = append(list, old)
	}
	list = append(list, r)
	for len(list) > h.cfg.MaxRadarsPerLogin {
		list[0].kick("replaced", "too many radars for this login")
		list = list[1:]
	}
	h.radars[k] = list
	h.dirty = true
}

// unregister reports whether the login has no radar left.
func (h *Hub) unregister(r *radar) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	k := key(r.login)
	list := h.radars[k][:0:0]
	for _, x := range h.radars[k] {
		if x != r {
			list = append(list, x)
		}
	}
	if len(list) == 0 {
		delete(h.radars, k)
	} else {
		h.radars[k] = list
	}
	h.dirty = true
	return len(list) == 0
}

func (h *Hub) markDirty() {
	h.mu.Lock()
	h.dirty = true
	h.mu.Unlock()
}

func (h *Hub) kickLogin(login, code, message string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	list := h.radars[key(login)]
	for _, r := range list {
		r.kick(code, message)
	}
	delete(h.radars, key(login))
	h.dirty = true
	return len(list)
}

func (h *Hub) closeAll(code websocket.StatusCode, reason string) {
	var conns []*websocket.Conn
	h.mu.Lock()
	for _, list := range h.radars {
		for _, r := range list {
			conns = append(conns, r.conn)
		}
	}
	h.mu.Unlock()
	var wg sync.WaitGroup
	for _, c := range conns {
		wg.Add(1)
		go func() { defer wg.Done(); _ = c.Close(code, reason) }()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
	}
}

func (r *radar) kick(code, message string) {
	r.kickOnce.Do(func() { go fail(r.conn, code, message) })
}

// failNow is kick for the radar's own session, which must not return before the
// error is written.
func (r *radar) failNow(code, message string) {
	r.kickOnce.Do(func() { fail(r.conn, code, message) })
}

func fail(c *websocket.Conn, code, message string) {
	data, _ := json.Marshal(errorMessage{Type: "error", Code: code, Message: message})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = c.Write(ctx, websocket.MessageText, data)
	cancel()
	_ = c.Close(closeCodes[code], code)
}

func writeJSON(ctx context.Context, c *websocket.Conn, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return c.Write(ctx, websocket.MessageText, data)
}

// Presence.

func (h *Hub) presence() presenceMessage {
	members := h.store.Snapshot()
	msg := presenceMessage{Type: "presence", Online: []presenceOnline{}, Recent: []presenceRecent{}}
	online := map[string]bool{}

	h.mu.Lock()
	for _, list := range h.radars {
		if len(list) == 0 {
			continue
		}
		entry := presenceOnline{Login: list[0].login, AvatarURL: list[0].avatarURL, Since: stamp(list[0].since), Radars: []presenceRadar{}}
		for _, r := range list {
			entry.Tabs += r.tabs
			entry.Radars = append(entry.Radars, presenceRadar{Commit: r.version.Commit, Behind: r.version.Behind, Tabs: r.tabs, Since: stamp(r.since)})
		}
		online[key(entry.Login)] = true
		msg.Online = append(msg.Online, entry)
	}
	h.mu.Unlock()

	sort.Slice(msg.Online, func(i, j int) bool { return key(msg.Online[i].Login) < key(msg.Online[j].Login) })

	cutoff := h.now().Add(-h.cfg.RecentWindow)
	for _, m := range members {
		if online[key(m.Login)] || m.Banned || m.activeTokens() == 0 || m.LastSeen.Before(cutoff) {
			continue
		}
		msg.Recent = append(msg.Recent, presenceRecent{Login: m.Login, AvatarURL: m.AvatarURL, LastSeen: stamp(m.LastSeen)})
	}
	sort.Slice(msg.Recent, func(i, j int) bool { return msg.Recent[i].LastSeen > msg.Recent[j].LastSeen })
	return msg
}

func (h *Hub) broadcaster(ctx context.Context) {
	t := time.NewTicker(h.cfg.PresenceThrottle)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		h.mu.Lock()
		if h.dirty {
			h.dirty = false
			for _, list := range h.radars {
				for _, r := range list {
					select {
					case r.notify <- struct{}{}:
					default:
					}
				}
			}
		}
		h.mu.Unlock()
	}
}

func (h *Hub) writer(ctx context.Context, r *radar) {
	var last []byte
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.notify:
		}
		data, err := json.Marshal(h.presence())
		if err != nil || bytes.Equal(data, last) {
			continue
		}
		wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = r.conn.Write(wctx, websocket.MessageText, data)
		cancel()
		if err != nil {
			r.conn.CloseNow()
			return
		}
		last = data
	}
}

func (h *Hub) keepalive(ctx context.Context, c *websocket.Conn) {
	t := time.NewTicker(h.cfg.PingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		pctx, cancel := context.WithTimeout(ctx, h.cfg.PongWait)
		err := c.Ping(pctx)
		cancel()
		if err != nil {
			c.CloseNow()
			return
		}
	}
}

// One connection.

type frame struct {
	data   []byte
	binary bool
}

type session struct {
	h      *Hub
	c      *websocket.Conn
	ctx    context.Context
	ip     string
	frames chan frame
	count  int
	window time.Time
}

func (h *Hub) ServeWS(w http.ResponseWriter, req *http.Request) {
	ip := clientIP(req)
	if !h.acquireIP(ip) {
		http.Error(w, "too many connections", http.StatusTooManyRequests)
		return
	}
	defer h.releaseIP(ip)

	// No Origin check: radars are not browsers, and a page that opened this socket from
	// a browser would still need a hub token, which browsers never hold.
	c, err := websocket.Accept(w, req, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	defer c.CloseNow()
	c.SetReadLimit(h.cfg.MaxMessage)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	frames := make(chan frame)
	go func() {
		defer close(frames)
		for {
			typ, data, err := c.Read(ctx)
			if err != nil {
				return
			}
			select {
			case frames <- frame{data: data, binary: typ != websocket.MessageText}:
			case <-ctx.Done():
				return
			}
		}
	}()
	go h.keepalive(ctx, c)

	s := &session{h: h, c: c, ctx: ctx, ip: ip, frames: frames}
	s.run()
}

// next waits for a frame. ok is false when the connection is gone or the timeout hit.
func (s *session) next(timeout <-chan time.Time) (f frame, ok bool, timedOut bool) {
	select {
	case f, ok = <-s.frames:
		return f, ok, false
	case <-timeout:
		return f, false, true
	case <-s.ctx.Done():
		return f, false, false
	}
}

func (s *session) parse(f frame) (inMessage, bool) {
	var m inMessage
	if f.binary || json.Unmarshal(f.data, &m) != nil || m.Type == "" || !m.validate() {
		return m, false
	}
	return m, true
}

func (s *session) allow() bool {
	now := s.h.now()
	if now.Sub(s.window) >= time.Minute {
		s.window, s.count = now, 0
	}
	s.count++
	return s.count <= s.h.cfg.MaxMessagesPerMinute
}

func (s *session) fail(code, message string) { fail(s.c, code, message) }

func (s *session) run() {
	timer := time.NewTimer(s.h.cfg.HelloTimeout)
	f, ok, timedOut := s.next(timer.C)
	timer.Stop()
	if timedOut {
		s.fail("hello_timeout", "no hello received in time")
		return
	}
	if !ok {
		return
	}
	hello, valid := s.parse(f)
	if !valid || hello.Type != "hello" || hello.Protocol == nil {
		s.fail("bad_message", "the first message must be a valid hello")
		return
	}
	if *hello.Protocol != protocolVersion {
		s.fail("protocol_unsupported", "this hub speaks protocol 1")
		return
	}
	if !instanceIDPattern.MatchString(hello.InstanceID) {
		s.fail("bad_message", "invalid instanceId")
		return
	}

	r := &radar{conn: s.c, instanceID: hello.InstanceID, notify: make(chan struct{}, 1)}
	r.applyStatus(hello)

	var issued string
	if hello.HubToken != nil && *hello.HubToken != "" {
		m, res := s.h.store.Lookup(*hello.HubToken)
		switch res {
		case tokenUnknown:
			s.fail("bad_token", "unknown hub token")
			return
		case tokenRevoked:
			s.fail("revoked", "this hub token was revoked")
			return
		}
		r.login, r.avatarURL = m.Login, m.AvatarURL
	} else {
		var code, message string
		r.login, r.avatarURL, issued, code, message = s.signIn(r)
		if code != "" {
			s.fail(code, message)
			return
		}
		if r.login == "" {
			return
		}
	}

	r.since = s.h.now()
	s.h.register(r)
	defer func() {
		if s.h.unregister(r) {
			_ = s.h.store.Touch(r.login, &r.version)
		}
	}()
	_ = s.h.store.Touch(r.login, &r.version)

	err := writeJSON(s.ctx, s.c, welcomeMessage{Type: "welcome", Protocol: protocolVersion, Login: r.login, AvatarURL: r.avatarURL, HubToken: issued})
	if err != nil {
		return
	}
	s.h.log.Printf("online: %s (%s)", r.login, shortID(r.instanceID))
	defer s.h.log.Printf("offline: %s (%s)", r.login, shortID(r.instanceID))

	go s.h.writer(s.ctx, r)
	r.notify <- struct{}{}

	for {
		f, ok, _ := s.next(nil)
		if !ok {
			return
		}
		if !s.allow() {
			r.failNow("rate_limited", "too many messages")
			return
		}
		m, valid := s.parse(f)
		if !valid {
			r.failNow("bad_message", "invalid message")
			return
		}
		if m.Type == "status" {
			s.h.mu.Lock()
			r.applyStatus(m)
			s.h.dirty = true
			s.h.mu.Unlock()
		}
	}
}

func (r *radar) applyStatus(m inMessage) {
	if m.Tabs != nil {
		r.tabs = *m.Tabs
	}
	if m.Version != nil {
		r.version = *m.Version
	}
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// signIn runs GitHub's device flow for this connection. On success it returns the
// login, avatar and the newly issued hub token; otherwise an error code.
func (s *session) signIn(r *radar) (login, avatar, token, code, message string) {
	if !s.h.acquirePending(s.ip) {
		return "", "", "", "rate_limited", "too many sign-ins waiting from this address"
	}
	defer s.h.releasePending(s.ip)

	dev, err := s.h.gh.StartDevice(s.ctx)
	if err != nil {
		s.h.log.Printf("sign-in: device code: %v", err)
		return "", "", "", "auth_unavailable", "GitHub sign-in is unavailable"
	}
	if writeJSON(s.ctx, s.c, authRequiredMessage{Type: "auth_required", UserCode: dev.UserCode, VerificationURI: dev.VerificationURI, ExpiresIn: dev.ExpiresIn}) != nil {
		return
	}

	interval := max(time.Duration(dev.Interval)*time.Second, s.h.cfg.MinPollInterval)
	deadline := time.NewTimer(time.Duration(dev.ExpiresIn) * time.Second)
	defer deadline.Stop()
	var userToken string
	for userToken == "" {
		tick := time.NewTimer(interval)
		select {
		case <-s.ctx.Done():
			tick.Stop()
			return
		case <-deadline.C:
			tick.Stop()
			return "", "", "", "auth_expired", "the sign-in code expired"
		case f, ok := <-s.frames:
			tick.Stop()
			if !ok {
				return
			}
			if !s.allow() {
				return "", "", "", "rate_limited", "too many messages"
			}
			if m, valid := s.parse(f); !valid {
				return "", "", "", "bad_message", "invalid message"
			} else if m.Type == "status" {
				r.applyStatus(m)
			}
			continue
		case <-tick.C:
		}
		userToken, err = s.h.gh.PollDevice(s.ctx, dev.DeviceCode)
		switch {
		case err == nil:
		case errors.Is(err, errPending):
		case errors.Is(err, errSlowDown):
			interval += 5 * time.Second
		case errors.Is(err, errExpired):
			return "", "", "", "auth_expired", "the sign-in code expired"
		case errors.Is(err, errDenied):
			return "", "", "", "auth_denied", "sign-in cancelled on GitHub"
		default:
			s.h.log.Printf("sign-in: poll: %v", err)
			return "", "", "", "auth_unavailable", "GitHub sign-in is unavailable"
		}
	}

	user, err := s.h.gh.User(s.ctx, userToken)
	userToken = ""
	if err != nil {
		s.h.log.Printf("sign-in: user: %v", err)
		return "", "", "", "auth_unavailable", "could not read the GitHub identity"
	}
	if s.h.store.IsBanned(user.Login) {
		s.h.log.Printf("sign-in refused: %s is revoked", user.Login)
		return "", "", "", "revoked", user.Login + " was revoked on this hub"
	}
	member, err := s.h.gh.IsMember(s.ctx, user.Login)
	if err != nil {
		s.h.log.Printf("sign-in: membership of %s: %v", user.Login, err)
		return "", "", "", "auth_unavailable", "could not check the organisation membership"
	}
	if !member {
		s.h.log.Printf("sign-in refused: %s is not a member of %s", user.Login, s.h.cfg.Org)
		return "", "", "", "not_member", user.Login + " is not an active member of " + s.h.cfg.Org
	}
	token, err = s.h.store.IssueToken(user.Login, user.AvatarURL, "github")
	if err != nil {
		s.h.log.Printf("sign-in: store: %v", err)
		return "", "", "", "auth_unavailable", "the hub could not save the sign-in"
	}
	s.h.log.Printf("signed in: %s", user.Login)
	return user.Login, user.AvatarURL, token, "", ""
}

// Membership re-check.

func (h *Hub) revalidate(ctx context.Context) {
	if err := h.gh.CheckToken(ctx); err != nil {
		h.log.Printf("revalidate: org token: %v", err)
		return
	}
	for _, login := range h.store.GitHubLogins() {
		member, err := h.gh.IsMember(ctx, login)
		if err != nil {
			// A doubt never revokes anyone: wait for the next round.
			h.log.Printf("revalidate: %s: %v", login, err)
			return
		}
		if member {
			continue
		}
		n, err := h.store.Revoke(login, "left_org", false)
		if err != nil {
			h.log.Printf("revalidate: revoke %s: %v", login, err)
			continue
		}
		h.kickLogin(login, "revoked", login+" is no longer a member of "+h.cfg.Org)
		h.log.Printf("revalidate: %s left %s, %d token(s) revoked", login, h.cfg.Org, n)
	}
}

func (h *Hub) revalidator(ctx context.Context) {
	first := time.NewTimer(time.Minute)
	defer first.Stop()
	select {
	case <-ctx.Done():
		return
	case <-first.C:
	}
	h.revalidate(ctx)
	t := time.NewTicker(h.cfg.RevalidateEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			h.revalidate(ctx)
		}
	}
}

func (h *Hub) touchOnline() {
	type seen struct {
		login   string
		version Version
	}
	var list []seen
	h.mu.Lock()
	for _, rs := range h.radars {
		for _, r := range rs {
			list = append(list, seen{r.login, r.version})
		}
	}
	h.mu.Unlock()
	for _, x := range list {
		_ = h.store.Touch(x.login, &x.version)
	}
}
