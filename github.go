package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	errPending  = errors.New("authorization pending")
	errSlowDown = errors.New("slow down")
	errExpired  = errors.New("device code expired")
	errDenied   = errors.New("access denied by the user")
)

type GitHub struct {
	WebURL    string
	APIURL    string
	ClientID  string
	Org       string
	TokenFile string
	HTTP      *http.Client
	now       func() time.Time

	mu        sync.Mutex
	expiry    time.Time
	checked   bool
	tokenErr  error
	checkedAt time.Time
}

type DeviceCode struct {
	DeviceCode      string `json:"device_code"`
	UserCode        string `json:"user_code"`
	VerificationURI string `json:"verification_uri"`
	ExpiresIn       int    `json:"expires_in"`
	Interval        int    `json:"interval"`
}

func newGitHub(cfg Config, now func() time.Time) *GitHub {
	return &GitHub{
		WebURL:    strings.TrimRight(cfg.GitHubWebURL, "/"),
		APIURL:    strings.TrimRight(cfg.GitHubAPIURL, "/"),
		ClientID:  cfg.ClientID,
		Org:       cfg.Org,
		TokenFile: cfg.TokenFile,
		HTTP:      &http.Client{Timeout: 15 * time.Second},
		now:       now,
	}
}

func (g *GitHub) postForm(ctx context.Context, path string, form url.Values, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.WebURL+path, strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pr-radar-hub")
	resp, err := g.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("POST %s: HTTP %d", path, resp.StatusCode)
	}
	return json.Unmarshal(body, out)
}

func (g *GitHub) StartDevice(ctx context.Context) (DeviceCode, error) {
	var d DeviceCode
	if g.ClientID == "" {
		return d, errors.New("HUB_GITHUB_CLIENT_ID is not set")
	}
	var raw struct {
		DeviceCode
		Error string `json:"error"`
	}
	if err := g.postForm(ctx, "/login/device/code", url.Values{"client_id": {g.ClientID}}, &raw); err != nil {
		return d, err
	}
	if raw.Error != "" || raw.DeviceCode.DeviceCode == "" {
		return d, fmt.Errorf("device code refused: %s", raw.Error)
	}
	return raw.DeviceCode, nil
}

// PollDevice returns the user's GitHub token once they approved, or one of the
// errPending, errSlowDown, errExpired, errDenied sentinels.
func (g *GitHub) PollDevice(ctx context.Context, deviceCode string) (string, error) {
	var raw struct {
		AccessToken string `json:"access_token"`
		Error       string `json:"error"`
	}
	form := url.Values{
		"client_id":   {g.ClientID},
		"device_code": {deviceCode},
		"grant_type":  {"urn:ietf:params:oauth:grant-type:device_code"},
	}
	if err := g.postForm(ctx, "/login/oauth/access_token", form, &raw); err != nil {
		return "", err
	}
	switch raw.Error {
	case "":
		if raw.AccessToken == "" {
			return "", errors.New("empty access token")
		}
		return raw.AccessToken, nil
	case "authorization_pending":
		return "", errPending
	case "slow_down":
		return "", errSlowDown
	case "expired_token":
		return "", errExpired
	case "access_denied":
		return "", errDenied
	default:
		return "", fmt.Errorf("device flow: %s", raw.Error)
	}
}

func (g *GitHub) get(ctx context.Context, path, token string) (*http.Response, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, g.APIURL+path, nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("User-Agent", "pr-radar-hub")
	resp, err := g.HTTP.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp, body, err
}

type ghUser struct {
	Login     string `json:"login"`
	AvatarURL string `json:"avatar_url"`
}

// User reads who owns a user token. The token is not kept anywhere.
func (g *GitHub) User(ctx context.Context, userToken string) (ghUser, error) {
	var u ghUser
	resp, body, err := g.get(ctx, "/user", userToken)
	if err != nil {
		return u, err
	}
	if resp.StatusCode != http.StatusOK {
		return u, fmt.Errorf("GET /user: HTTP %d", resp.StatusCode)
	}
	if err := json.Unmarshal(body, &u); err != nil {
		return u, err
	}
	if u.Login == "" {
		return u, errors.New("GET /user: no login")
	}
	return u, nil
}

func (g *GitHub) orgToken() (string, error) {
	data, err := os.ReadFile(g.TokenFile)
	if err != nil {
		return "", fmt.Errorf("org token: %w", err)
	}
	t := strings.TrimSpace(string(data))
	if t == "" {
		return "", errors.New("org token: empty file")
	}
	return t, nil
}

func (g *GitHub) orgGet(ctx context.Context, path string) (*http.Response, []byte, error) {
	token, err := g.orgToken()
	if err != nil {
		g.record(nil, err)
		return nil, nil, err
	}
	resp, body, err := g.get(ctx, path, token)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode == http.StatusUnauthorized {
		err = errors.New("org token rejected by GitHub (expired or revoked?)")
		g.record(resp, err)
		return nil, nil, err
	}
	g.record(resp, nil)
	return resp, body, nil
}

func (g *GitHub) record(resp *http.Response, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.checked = true
	g.checkedAt = g.now()
	g.tokenErr = err
	if resp == nil {
		return
	}
	g.expiry = time.Time{}
	if v := resp.Header.Get("Github-Authentication-Token-Expiration"); v != "" {
		if t, perr := time.Parse("2006-01-02 15:04:05 MST", v); perr == nil {
			g.expiry = t.UTC()
		}
	}
}

// IsMember tells whether login is an active member of the org. A false with a nil
// error is GitHub's definite answer; any doubt is an error.
func (g *GitHub) IsMember(ctx context.Context, login string) (bool, error) {
	resp, body, err := g.orgGet(ctx, "/orgs/"+url.PathEscape(g.Org)+"/memberships/"+url.PathEscape(login))
	if err != nil {
		return false, err
	}
	switch resp.StatusCode {
	case http.StatusOK:
		var m struct {
			State string `json:"state"`
		}
		if err := json.Unmarshal(body, &m); err != nil {
			return false, err
		}
		return m.State == "active", nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("membership of %s: HTTP %d", login, resp.StatusCode)
	}
}

func (g *GitHub) LookupUser(ctx context.Context, login string) (ghUser, error) {
	var u ghUser
	resp, body, err := g.orgGet(ctx, "/users/"+url.PathEscape(login))
	if err != nil {
		return u, err
	}
	if resp.StatusCode != http.StatusOK {
		return u, fmt.Errorf("no GitHub user %q (HTTP %d)", login, resp.StatusCode)
	}
	err = json.Unmarshal(body, &u)
	return u, err
}

func (g *GitHub) CheckToken(ctx context.Context) error {
	resp, _, err := g.orgGet(ctx, "/rate_limit")
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("GET /rate_limit: HTTP %d", resp.StatusCode)
		g.record(resp, err)
	}
	return err
}

type tokenStatus struct {
	OK        bool       `json:"ok"`
	Checked   bool       `json:"checked"`
	Expires   *time.Time `json:"expires,omitempty"`
	DaysLeft  *int       `json:"daysLeft,omitempty"`
	Error     string     `json:"error,omitempty"`
	CheckedAt *time.Time `json:"checkedAt,omitempty"`
}

func (g *GitHub) Status(warning time.Duration) tokenStatus {
	g.mu.Lock()
	defer g.mu.Unlock()
	s := tokenStatus{Checked: g.checked}
	if g.checked {
		at := g.checkedAt.UTC()
		s.CheckedAt = &at
	}
	if g.tokenErr != nil {
		s.Error = g.tokenErr.Error()
	}
	if !g.expiry.IsZero() {
		exp := g.expiry
		days := int(exp.Sub(g.now()).Hours() / 24)
		s.Expires, s.DaysLeft = &exp, &days
	}
	s.OK = g.checked && g.tokenErr == nil && (g.expiry.IsZero() || g.expiry.Sub(g.now()) > warning)
	return s
}
