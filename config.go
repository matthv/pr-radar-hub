package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type Config struct {
	Addr         string
	Org          string
	DataDir      string
	ClientID     string
	TokenFile    string
	GitHubWebURL string
	GitHubAPIURL string

	HelloTimeout     time.Duration
	PingInterval     time.Duration
	PongWait         time.Duration
	PresenceThrottle time.Duration
	MinPollInterval  time.Duration
	RevalidateEvery  time.Duration
	RecentWindow     time.Duration
	TokenWarning     time.Duration

	MaxMessage           int64
	MaxThemeMessage      int64
	MaxPerIP             int
	MaxPendingPerIP      int
	MaxRadarsPerLogin    int
	MaxMessagesPerMinute int
}

func defaultConfig() Config {
	return Config{
		Addr:                 "127.0.0.1:8795",
		Org:                  "ForestAdmin",
		GitHubWebURL:         "https://github.com",
		GitHubAPIURL:         "https://api.github.com",
		HelloTimeout:         10 * time.Second,
		PingInterval:         30 * time.Second,
		PongWait:             45 * time.Second,
		PresenceThrottle:     time.Second,
		MinPollInterval:      5 * time.Second,
		RevalidateEvery:      24 * time.Hour,
		RecentWindow:         14 * 24 * time.Hour,
		TokenWarning:         30 * 24 * time.Hour,
		MaxMessage:           16 << 10,
		MaxThemeMessage:      110 << 10,
		MaxPerIP:             64,
		MaxPendingPerIP:      5,
		MaxRadarsPerLogin:    10,
		MaxMessagesPerMinute: 30,
	}
}

func defaultDataDir() string {
	exe, err := os.Executable()
	if err != nil {
		return "data"
	}
	return filepath.Join(filepath.Dir(exe), "data")
}

func configFromEnv() (Config, error) {
	cfg := defaultConfig()
	cfg.DataDir = defaultDataDir()
	home, _ := os.UserHomeDir()
	cfg.TokenFile = filepath.Join(home, ".config", "radar-hub", "github-token")

	if v := os.Getenv("HUB_PORT"); v != "" {
		port, err := strconv.Atoi(v)
		if err != nil || port < 1 || port > 65535 {
			return cfg, fmt.Errorf("HUB_PORT: invalid port %q", v)
		}
		cfg.Addr = fmt.Sprintf("127.0.0.1:%d", port)
	}
	if v := os.Getenv("HUB_ORG"); v != "" {
		cfg.Org = v
	}
	if v := os.Getenv("HUB_DATA_DIR"); v != "" {
		cfg.DataDir = v
	}
	if v := os.Getenv("HUB_GITHUB_CLIENT_ID"); v != "" {
		cfg.ClientID = v
	}
	if v := os.Getenv("HUB_GITHUB_TOKEN_FILE"); v != "" {
		cfg.TokenFile = v
	}
	return cfg, nil
}

func (c Config) adminSocket() string {
	return filepath.Join(c.DataDir, "admin.sock")
}
