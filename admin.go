package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"
)

// The admin API listens on a unix socket in the data directory, readable by the
// hub's user only: the reverse proxy cannot reach it.

type statusRadar struct {
	Commit *string   `json:"commit"`
	Behind *int      `json:"behind"`
	Tabs   int       `json:"tabs"`
	Since  time.Time `json:"since"`
}

type statusMember struct {
	Login        string        `json:"login"`
	Online       bool          `json:"online"`
	Radars       []statusRadar `json:"radars,omitempty"`
	LastSeen     time.Time     `json:"lastSeen"`
	LastVersion  *Version      `json:"lastVersion,omitempty"`
	ActiveTokens int           `json:"activeTokens"`
	Sources      []string      `json:"sources,omitempty"`
	Banned       bool          `json:"banned,omitempty"`
	Revoked      bool          `json:"revoked,omitempty"`
}

type statusReport struct {
	Org         string         `json:"org"`
	GitHubToken tokenStatus    `json:"githubToken"`
	Members     []statusMember `json:"members"`
}

func (h *Hub) report() statusReport {
	rep := statusReport{Org: h.cfg.Org, GitHubToken: h.gh.Status(h.cfg.TokenWarning)}
	h.mu.Lock()
	online := map[string][]statusRadar{}
	for k, list := range h.radars {
		for _, r := range list {
			online[k] = append(online[k], statusRadar{Commit: r.version.Commit, Behind: r.version.Behind, Tabs: r.tabs, Since: r.since.UTC()})
		}
	}
	h.mu.Unlock()
	for _, m := range h.store.Snapshot() {
		sm := statusMember{Login: m.Login, LastSeen: m.LastSeen, LastVersion: m.LastVersion, Banned: m.Banned, ActiveTokens: m.activeTokens()}
		sources := map[string]bool{}
		for _, t := range m.Tokens {
			if t.RevokedAt == nil {
				sources[t.Source] = true
			}
		}
		for s := range sources {
			sm.Sources = append(sm.Sources, s)
		}
		sort.Strings(sm.Sources)
		sm.Revoked = len(m.Tokens) > 0 && sm.ActiveTokens == 0
		if rs, ok := online[key(m.Login)]; ok {
			sm.Online, sm.Radars = true, rs
		}
		rep.Members = append(rep.Members, sm)
	}
	return rep
}

func (h *Hub) adminHandler() http.Handler {
	mux := http.NewServeMux()
	reply := func(w http.ResponseWriter, status int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(v)
	}
	problem := func(w http.ResponseWriter, status int, err string) {
		reply(w, status, map[string]string{"error": err})
	}
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		reply(w, 200, h.report())
	})
	mux.HandleFunc("POST /revoke", func(w http.ResponseWriter, r *http.Request) {
		login := r.URL.Query().Get("login")
		if login == "" {
			problem(w, 400, "login is required")
			return
		}
		n, err := h.store.Revoke(login, "admin", true)
		if err != nil {
			problem(w, 500, err.Error())
			return
		}
		closed := h.kickLogin(login, "revoked", login+" was revoked on this hub")
		h.log.Printf("admin: revoked %s (%d token(s), %d connection(s))", login, n, closed)
		reply(w, 200, map[string]int{"tokens": n, "connections": closed})
	})
	mux.HandleFunc("POST /unrevoke", func(w http.ResponseWriter, r *http.Request) {
		login := r.URL.Query().Get("login")
		ok, err := h.store.Unban(login)
		if err != nil {
			problem(w, 500, err.Error())
			return
		}
		h.log.Printf("admin: unrevoked %s", login)
		reply(w, 200, map[string]bool{"unbanned": ok})
	})
	mux.HandleFunc("POST /invite", func(w http.ResponseWriter, r *http.Request) {
		login := r.URL.Query().Get("login")
		if login == "" {
			problem(w, 400, "login is required")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()
		u, err := h.gh.LookupUser(ctx, login)
		if err != nil {
			problem(w, 400, err.Error())
			return
		}
		if _, err := h.store.Unban(u.Login); err != nil {
			problem(w, 500, err.Error())
			return
		}
		token, err := h.store.IssueToken(u.Login, u.AvatarURL, "invite")
		if err != nil {
			problem(w, 500, err.Error())
			return
		}
		h.log.Printf("admin: invited %s", u.Login)
		reply(w, 200, map[string]string{"login": u.Login, "hubToken": token})
	})
	return mux
}

func listenAdmin(path string) (net.Listener, error) {
	_ = os.Remove(path)
	l, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	// The data directory is 0700 already; this is belt and braces.
	return l, os.Chmod(path, 0o600)
}

// CLI side.

func adminClient(cfg Config) *http.Client {
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", cfg.adminSocket())
			},
		},
	}
}

func adminCall(cfg Config, method, path string, out any) error {
	req, err := http.NewRequest(method, "http://hub"+path, nil)
	if err != nil {
		return err
	}
	resp, err := adminClient(cfg).Do(req)
	if err != nil {
		return fmt.Errorf("is the hub running? (%s): %w", cfg.adminSocket(), err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		var e struct{ Error string }
		_ = json.Unmarshal(body, &e)
		return errors.New(e.Error)
	}
	return json.Unmarshal(body, out)
}

func ago(t time.Time) string {
	if t.IsZero() {
		return "never"
	}
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	}
}

func versionText(commit *string, behind *int) string {
	c := "?"
	if commit != nil && len(*commit) >= 7 {
		c = (*commit)[:7]
	}
	switch {
	case behind == nil:
		return c
	case *behind == 0:
		return c + " (up to date)"
	default:
		return fmt.Sprintf("%s (%d behind)", c, *behind)
	}
}

func cmdStatus(cfg Config, asJSON bool) error {
	var rep statusReport
	if err := adminCall(cfg, "GET", "/status", &rep); err != nil {
		return err
	}
	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(rep)
	}
	t := rep.GitHubToken
	switch {
	case !t.Checked:
		fmt.Println("GitHub token: not checked yet")
	case t.Error != "":
		fmt.Printf("GitHub token: PROBLEM: %s\n", t.Error)
	case t.Expires != nil:
		mark := "ok"
		if !t.OK {
			mark = "RENEW SOON"
		}
		fmt.Printf("GitHub token: %s, expires %s (%d days)\n", mark, t.Expires.Format("2006-01-02"), *t.DaysLeft)
	default:
		fmt.Println("GitHub token: ok, no expiry")
	}
	fmt.Println()

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "LOGIN\tSTATE\tVERSION\tTABS\tSEEN\tACCESS")
	for _, m := range rep.Members {
		access := strings.Join(m.Sources, ",")
		switch {
		case m.Banned:
			access = "revoked by admin"
		case m.Revoked:
			access = "revoked"
		}
		if m.Online {
			for i, r := range m.Radars {
				state, login := "online", m.Login
				if i > 0 {
					state, login = "", ""
				}
				fmt.Fprintf(w, "%s\t%s\t%s\t%d\tsince %s\t%s\n", login, state, versionText(r.Commit, r.Behind), r.Tabs, r.Since.Local().Format("15:04"), access)
			}
			continue
		}
		v := "?"
		if m.LastVersion != nil {
			v = versionText(m.LastVersion.Commit, m.LastVersion.Behind)
		}
		fmt.Fprintf(w, "%s\toffline\t%s\t-\t%s\t%s\n", m.Login, v, ago(m.LastSeen), access)
	}
	return w.Flush()
}

func cmdAdminAction(cfg Config, action, login string) error {
	path := "/" + action + "?login=" + url.QueryEscape(login)
	switch action {
	case "revoke":
		var out map[string]int
		if err := adminCall(cfg, "POST", path, &out); err != nil {
			return err
		}
		fmt.Printf("%s revoked: %d token(s), %d connection(s) closed. A new sign-in is refused until `unrevoke`.\n", login, out["tokens"], out["connections"])
	case "unrevoke":
		var out map[string]bool
		if err := adminCall(cfg, "POST", path, &out); err != nil {
			return err
		}
		if out["unbanned"] {
			fmt.Printf("%s may sign in again.\n", login)
		} else {
			fmt.Printf("%s was not revoked.\n", login)
		}
	case "invite":
		var out map[string]string
		if err := adminCall(cfg, "POST", path, &out); err != nil {
			return err
		}
		fmt.Printf("Hub token for %s (shown once, send it privately):\n\n%s\n\nOn their machine: save it in ~/.config/pr-radar/hub-token (mode 600).\n", out["login"], out["hubToken"])
	}
	return nil
}
