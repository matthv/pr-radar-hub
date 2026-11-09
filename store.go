package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type Version struct {
	Commit *string `json:"commit"`
	Behind *int    `json:"behind"`
}

type Token struct {
	Hash         string     `json:"hash"`
	Source       string     `json:"source"`
	IssuedAt     time.Time  `json:"issuedAt"`
	RevokedAt    *time.Time `json:"revokedAt,omitempty"`
	RevokeReason string     `json:"revokeReason,omitempty"`
}

type Member struct {
	Login       string    `json:"login"`
	AvatarURL   string    `json:"avatarUrl"`
	FirstSeen   time.Time `json:"firstSeen"`
	LastSeen    time.Time `json:"lastSeen"`
	LastVersion *Version  `json:"lastVersion,omitempty"`
	Banned      bool      `json:"banned,omitempty"`
	Tokens      []Token   `json:"tokens"`
}

func (m *Member) activeTokens() int {
	n := 0
	for _, t := range m.Tokens {
		if t.RevokedAt == nil {
			n++
		}
	}
	return n
}

type storeFile struct {
	Version int                `json:"version"`
	Members map[string]*Member `json:"members"`
}

type Store struct {
	path string
	now  func() time.Time

	mu      sync.Mutex
	members map[string]*Member
}

type lookupResult int

const (
	tokenUnknown lookupResult = iota
	tokenValid
	tokenRevoked
)

func key(login string) string { return strings.ToLower(login) }

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func newToken() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return "rh_" + base64.RawURLEncoding.EncodeToString(b)
}

func openStore(dir string, now func() time.Time) (*Store, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	s := &Store{path: filepath.Join(dir, "hub.json"), now: now, members: map[string]*Member{}}
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	var f storeFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, err
	}
	for _, m := range f.Members {
		s.members[key(m.Login)] = m
	}
	return s, nil
}

// Called with s.mu held.
func (s *Store) save() error {
	data, err := json.MarshalIndent(storeFile{Version: 1, Members: s.members}, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func (s *Store) member(login, avatarURL string) *Member {
	m := s.members[key(login)]
	if m == nil {
		m = &Member{Login: login, FirstSeen: s.now().UTC()}
		s.members[key(login)] = m
	}
	m.Login = login
	if avatarURL != "" {
		m.AvatarURL = avatarURL
	}
	return m
}

func (s *Store) IssueToken(login, avatarURL, source string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	token := newToken()
	m := s.member(login, avatarURL)
	m.Tokens = append(m.Tokens, Token{Hash: hashToken(token), Source: source, IssuedAt: s.now().UTC()})
	return token, s.save()
}

func (s *Store) Lookup(token string) (Member, lookupResult) {
	h := hashToken(token)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.members {
		for _, t := range m.Tokens {
			if t.Hash != h {
				continue
			}
			if t.RevokedAt != nil || m.Banned {
				return *m, tokenRevoked
			}
			return *m, tokenValid
		}
	}
	return Member{}, tokenUnknown
}

func (s *Store) IsBanned(login string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.members[key(login)]
	return m != nil && m.Banned
}

// Revoke revokes every active token of login and returns how many were revoked.
// ban also refuses any later sign-in until Unban.
func (s *Store) Revoke(login, reason string, ban bool) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.members[key(login)]
	if m == nil {
		return 0, nil
	}
	now := s.now().UTC()
	n := 0
	for i := range m.Tokens {
		if m.Tokens[i].RevokedAt == nil {
			m.Tokens[i].RevokedAt = &now
			m.Tokens[i].RevokeReason = reason
			n++
		}
	}
	if ban {
		m.Banned = true
	}
	return n, s.save()
}

func (s *Store) Unban(login string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.members[key(login)]
	if m == nil || !m.Banned {
		return false, nil
	}
	m.Banned = false
	return true, s.save()
}

func (s *Store) Touch(login string, v *Version) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.members[key(login)]
	if m == nil {
		return nil
	}
	m.LastSeen = s.now().UTC()
	if v != nil {
		cp := *v
		m.LastVersion = &cp
	}
	return s.save()
}

// GitHubLogins lists the members holding an active token issued by GitHub sign-in:
// the ones whose org membership is re-checked.
func (s *Store) GitHubLogins() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, m := range s.members {
		for _, t := range m.Tokens {
			if t.RevokedAt == nil && t.Source == "github" {
				out = append(out, m.Login)
				break
			}
		}
	}
	sort.Strings(out)
	return out
}

func (s *Store) Snapshot() []Member {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Member, 0, len(s.members))
	for _, m := range s.members {
		cp := *m
		cp.Tokens = append([]Token(nil), m.Tokens...)
		out = append(out, cp)
	}
	sort.Slice(out, func(i, j int) bool { return key(out[i].Login) < key(out[j].Login) })
	return out
}
