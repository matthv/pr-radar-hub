package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
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
	"unicode/utf8"

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
	loginPattern      = regexp.MustCompile(`^[A-Za-z0-9-]{1,39}$`)
	themeNamePattern  = regexp.MustCompile(`^[a-z0-9-]{1,40}$`)
	refPattern        = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

const (
	kudosPerPair    = time.Minute      // one kudos per sender -> recipient pair in this window
	offersPerMinute = 5                // theme offers per sender per minute
	wizzPerRef      = 10 * time.Minute // one wizz per sender -> recipient -> ref
	wizzPerPair     = 5 * time.Minute  // one plain wizz (no ref) per sender -> recipient
	nudgesPerPair   = 3                // wizzes with ref per sender -> recipient, all refs...
	nudgeWindow     = 10 * time.Minute // ...within this window
	outQueue        = 16               // relayed messages waiting for one radar
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
	// avail is each online login's last availability, whichever of its radars sent it.
	avail map[string]*string
	// noWizz holds the online logins that refuse plain wizzes (acceptsWizz false).
	noWizz map[string]bool
	// kudosSent and offersSent hold the recent sends for the relay limits.
	kudosSent  map[string]time.Time
	offersSent map[string][]time.Time
	// wizzUntil maps a sender/recipient(/ref) key to the end of its quiet period.
	wizzUntil map[string]time.Time
	// nudgesSent holds the recent wizzes with ref per sender/recipient pair.
	nudgesSent map[string][]time.Time

	// sessions counts the WebSocket handlers still running.
	sessions sync.WaitGroup
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
	out        chan []byte
	kickOnce   sync.Once
	// availability received before the radar was registered (hello, or status during sign-in).
	availSet bool
	avail    *string
	wizzSet  bool
	accepts  bool
}

func newHub(cfg Config, store *Store, gh *GitHub, logger *log.Logger) *Hub {
	return &Hub{
		cfg:        cfg,
		store:      store,
		gh:         gh,
		now:        time.Now,
		log:        logger,
		radars:     map[string][]*radar{},
		perIP:      map[string]int{},
		pending:    map[string]int{},
		avail:      map[string]*string{},
		kudosSent:  map[string]time.Time{},
		offersSent: map[string][]time.Time{},
		noWizz:     map[string]bool{},
		wizzUntil:  map[string]time.Time{},
		nudgesSent: map[string][]time.Time{},
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
	// Availability stays raw to tell an absent field (keep the value) from null.
	Availability json.RawMessage `json:"availability"`
	To           string          `json:"to"`
	Theme        json.RawMessage `json:"theme"`
	Ref          *string         `json:"ref"`
	// AcceptsWizz stays raw: absent, null or not a boolean keeps the current value.
	AcceptsWizz json.RawMessage `json:"acceptsWizz"`
}

// acceptsWizz reads the optional field: set is false unless it is true or false.
func (m *inMessage) acceptsWizz() (set bool, value bool) {
	var v *bool
	if len(m.AcceptsWizz) == 0 || json.Unmarshal(m.AcceptsWizz, &v) != nil || v == nil {
		return false, false
	}
	return true, *v
}

// availability reads the optional field: set is false when it is absent. Anything but
// "available" or "focus" counts as null.
func (m *inMessage) availability() (set bool, value *string) {
	if len(m.Availability) == 0 {
		return false, nil
	}
	var v string
	if json.Unmarshal(m.Availability, &v) == nil && (v == "available" || v == "focus") {
		return true, &v
	}
	return true, nil
}

// themeShape checks the form of a shared theme without reading its code.
type themeShape struct {
	Name  string  `json:"name"`
	Label *string `json:"label"`
	Icon  *string `json:"icon"`
	CSS   *string `json:"css"`
	JS    *string `json:"js"`
}

func validTheme(raw json.RawMessage) bool {
	var obj map[string]json.RawMessage
	if json.Unmarshal(raw, &obj) != nil || obj == nil {
		return false
	}
	var t themeShape
	if json.Unmarshal(raw, &t) != nil {
		return false
	}
	return themeNamePattern.MatchString(t.Name) &&
		t.Label != nil && utf8.RuneCountInString(*t.Label) <= 60 &&
		t.Icon != nil && utf8.RuneCountInString(*t.Icon) <= 16 &&
		t.CSS != nil
}

func (m *inMessage) validate() bool {
	switch m.Type {
	case "kudos":
		if !loginPattern.MatchString(m.To) {
			return false
		}
	case "wizz":
		if !loginPattern.MatchString(m.To) || (m.Ref != nil && !refPattern.MatchString(*m.Ref)) {
			return false
		}
	case "theme_offer":
		if !loginPattern.MatchString(m.To) || !validTheme(m.Theme) {
			return false
		}
	}
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
	Tag    *string `json:"tag"`
	Tabs   int     `json:"tabs"`
	Since  string  `json:"since"`
}

type presenceOnline struct {
	Login        string          `json:"login"`
	AvatarURL    string          `json:"avatarUrl"`
	Since        string          `json:"since"`
	Tabs         int             `json:"tabs"`
	Availability *string         `json:"availability"`
	AcceptsWizz  bool            `json:"acceptsWizz"`
	Radars       []presenceRadar `json:"radars"`
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

type kudosMessage struct {
	Type      string `json:"type"`
	From      string `json:"from"`
	AvatarURL string `json:"avatarUrl"`
	At        string `json:"at"`
}

type wizzMessage struct {
	Type      string  `json:"type"`
	From      string  `json:"from"`
	AvatarURL string  `json:"avatarUrl"`
	At        string  `json:"at"`
	Ref       *string `json:"ref,omitempty"`
}

type themeOfferMessage struct {
	Type      string          `json:"type"`
	ID        string          `json:"id"`
	From      string          `json:"from"`
	AvatarURL string          `json:"avatarUrl"`
	Theme     json.RawMessage `json:"theme"`
}

type undeliveredMessage struct {
	Type string `json:"type"`
	Kind string `json:"kind"`
	To   string `json:"to"`
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
	if r.availSet {
		h.avail[k] = r.avail
	}
	if r.wizzSet {
		h.setAcceptsWizz(k, r.accepts)
	}
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
		delete(h.avail, k)
		delete(h.noWizz, k)
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
	delete(h.avail, key(login))
	delete(h.noWizz, key(login))
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
		entry := presenceOnline{Login: list[0].login, AvatarURL: list[0].avatarURL, Since: stamp(list[0].since), Availability: h.avail[key(list[0].login)], AcceptsWizz: !h.noWizz[key(list[0].login)], Radars: []presenceRadar{}}
		for _, r := range list {
			entry.Tabs += r.tabs
			entry.Radars = append(entry.Radars, presenceRadar{Commit: r.version.Commit, Behind: r.version.Behind, Tag: r.version.Tag, Tabs: r.tabs, Since: stamp(r.since)})
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

// writer is the only goroutine writing to a registered radar after welcome: presence
// updates and relayed messages.
func (h *Hub) writer(ctx context.Context, r *radar) {
	var last []byte
	for {
		var data []byte
		select {
		case <-ctx.Done():
			return
		case data = <-r.out:
		case <-r.notify:
			p, err := json.Marshal(h.presence())
			if err != nil || bytes.Equal(p, last) {
				continue
			}
			data, last = p, p
		}
		wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := r.conn.Write(wctx, websocket.MessageText, data)
		cancel()
		if err != nil {
			r.conn.CloseNow()
			return
		}
	}
}

// queue hands a message to a radar's writer, dropping it if the radar is not keeping up.
func (r *radar) queue(data []byte) bool {
	select {
	case r.out <- data:
		return true
	default:
		return false
	}
}

// Relays. Their content is never stored nor logged.

// relay sends data to every radar of login `to`. It returns false when `to` is offline.
func (h *Hub) relay(to string, data []byte) bool {
	h.mu.Lock()
	list := append([]*radar(nil), h.radars[key(to)]...)
	h.mu.Unlock()
	delivered := false
	for _, r := range list {
		if r.queue(data) {
			delivered = true
		}
	}
	return delivered
}

func (h *Hub) isOnline(login string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.radars[key(login)]) > 0
}

// allowKudos applies the one-per-minute limit for a sender -> recipient pair.
func (h *Hub) allowKudos(from, to string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := h.now()
	for k, t := range h.kudosSent {
		if now.Sub(t) >= kudosPerPair {
			delete(h.kudosSent, k)
		}
	}
	k := key(from) + " " + key(to)
	if _, recent := h.kudosSent[k]; recent {
		return false
	}
	h.kudosSent[k] = now
	return true
}

// allowOffer applies the five-per-minute limit for one sender.
func (h *Hub) allowOffer(from string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := h.now()
	for k, times := range h.offersSent {
		kept := times[:0]
		for _, t := range times {
			if now.Sub(t) < time.Minute {
				kept = append(kept, t)
			}
		}
		if len(kept) == 0 {
			delete(h.offersSent, k)
		} else {
			h.offersSent[k] = kept
		}
	}
	k := key(from)
	if len(h.offersSent[k]) >= offersPerMinute {
		return false
	}
	h.offersSent[k] = append(h.offersSent[k], now)
	return true
}

func (h *Hub) handleKudos(r *radar, m inMessage) {
	if key(m.To) == key(r.login) {
		return
	}
	if !h.isOnline(m.To) {
		r.undelivered("kudos", m.To)
		return
	}
	if !h.allowKudos(r.login, m.To) {
		return
	}
	data, _ := json.Marshal(kudosMessage{Type: "kudos", From: r.login, AvatarURL: r.avatarURL, At: stamp(h.now())})
	if !h.relay(m.To, data) {
		r.undelivered("kudos", m.To)
	}
}

func (h *Hub) handleThemeOffer(r *radar, m inMessage) {
	if key(m.To) == key(r.login) {
		return
	}
	if !h.isOnline(m.To) {
		r.undelivered("theme_offer", m.To)
		return
	}
	if !h.allowOffer(r.login) {
		return
	}
	data, _ := json.Marshal(themeOfferMessage{Type: "theme_offer", ID: randomID(), From: r.login, AvatarURL: r.avatarURL, Theme: m.Theme})
	if int64(len(data)) > h.cfg.MaxThemeMessage+512 {
		// Only an unusually long avatar URL gets here: keep the documented bound.
		return
	}
	if !h.relay(m.To, data) {
		r.undelivered("theme_offer", m.To)
	}
}

// setAcceptsWizz must be called with h.mu held.
func (h *Hub) setAcceptsWizz(k string, accepts bool) {
	if accepts {
		delete(h.noWizz, k)
	} else {
		h.noWizz[k] = true
	}
}

// allowWizz checks the recipient's choice for plain wizzes, then the limits: 10 minutes
// per sender -> recipient -> ref and at most 3 nudges per pair in 10 minutes, all refs
// together; 5 minutes per sender -> recipient without ref. A dropped wizz uses nothing.
func (h *Hub) allowWizz(from, to string, ref *string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if ref == nil && h.noWizz[key(to)] {
		return false
	}
	now := h.now()
	for k, until := range h.wizzUntil {
		if !now.Before(until) {
			delete(h.wizzUntil, k)
		}
	}
	for k, times := range h.nudgesSent {
		kept := times[:0]
		for _, t := range times {
			if now.Sub(t) < nudgeWindow {
				kept = append(kept, t)
			}
		}
		if len(kept) == 0 {
			delete(h.nudgesSent, k)
		} else {
			h.nudgesSent[k] = kept
		}
	}
	pair := key(from) + " " + key(to)
	k, quiet := pair, wizzPerPair
	if ref != nil {
		k, quiet = pair+" "+*ref, wizzPerRef
	}
	if _, recent := h.wizzUntil[k]; recent {
		return false
	}
	if ref != nil {
		if len(h.nudgesSent[pair]) >= nudgesPerPair {
			return false
		}
		h.nudgesSent[pair] = append(h.nudgesSent[pair], now)
	}
	h.wizzUntil[k] = now.Add(quiet)
	return true
}

func (h *Hub) handleWizz(r *radar, m inMessage) {
	if key(m.To) == key(r.login) {
		return
	}
	if !h.isOnline(m.To) {
		r.undelivered("wizz", m.To)
		return
	}
	if !h.allowWizz(r.login, m.To, m.Ref) {
		return
	}
	data, _ := json.Marshal(wizzMessage{Type: "wizz", From: r.login, AvatarURL: r.avatarURL, At: stamp(h.now()), Ref: m.Ref})
	if !h.relay(m.To, data) {
		r.undelivered("wizz", m.To)
	}
}

func (r *radar) undelivered(kind, to string) {
	data, _ := json.Marshal(undeliveredMessage{Type: "undelivered", Kind: kind, To: to})
	r.queue(data)
}

func randomID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
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
	h.sessions.Add(1)
	defer h.sessions.Done()
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
	// Frames may reach the theme size; every other type is held to MaxMessage below.
	c.SetReadLimit(h.cfg.MaxThemeMessage)

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
			if int64(len(data)) > h.cfg.MaxMessage && (typ != websocket.MessageText || frameType(data) != "theme_offer") {
				_ = c.Close(websocket.StatusMessageTooBig, "message too big")
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

// frameType reads the type of a frame, "" if it has none.
func frameType(data []byte) string {
	var m struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(data, &m)
	return m.Type
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

	r := &radar{conn: s.c, instanceID: hello.InstanceID, notify: make(chan struct{}, 1), out: make(chan []byte, outQueue)}
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
		switch m.Type {
		case "status":
			s.h.mu.Lock()
			r.applyStatus(m)
			if set, v := m.availability(); set {
				s.h.avail[key(r.login)] = v
			}
			if set, v := m.acceptsWizz(); set {
				s.h.setAcceptsWizz(key(r.login), v)
			}
			s.h.dirty = true
			s.h.mu.Unlock()
		case "kudos":
			s.h.handleKudos(r, m)
		case "theme_offer":
			s.h.handleThemeOffer(r, m)
		case "wizz":
			s.h.handleWizz(r, m)
		}
	}
}

// applyStatus records tabs and version; availability and acceptsWizz are kept on the radar until it
// is registered, then belongs to the login (see run).
func (r *radar) applyStatus(m inMessage) {
	if m.Tabs != nil {
		r.tabs = *m.Tabs
	}
	if m.Version != nil {
		r.version = *m.Version
	}
	if set, v := m.availability(); set {
		r.availSet, r.avail = true, v
	}
	if set, v := m.acceptsWizz(); set {
		r.wizzSet, r.accepts = true, v
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
