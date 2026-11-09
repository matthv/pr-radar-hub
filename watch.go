package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/coder/websocket"
)

// watch plays a radar: it signs in, reports a tab count and prints every presence
// it receives. Typing a number and Enter sends it as the new tab count.

func watchDir() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".config", "radar-hub")
}

func readOrCreateInstanceID(path string) (string, error) {
	if data, err := os.ReadFile(path); err == nil {
		if id := strings.TrimSpace(string(data)); instanceIDPattern.MatchString(id) {
			return id, nil
		}
	}
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	id := hex.EncodeToString(b)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", err
	}
	return id, os.WriteFile(path, []byte(id+"\n"), 0o600)
}

func cmdWatch(args []string) error {
	fs := flag.NewFlagSet("watch", flag.ExitOnError)
	defURL := os.Getenv("HUB_URL")
	if defURL == "" {
		defURL = "wss://radar-hub.matthv.com/ws"
	}
	hubURL := fs.String("url", defURL, "hub WebSocket URL (or HUB_URL)")
	tabs := fs.Int("tabs", 1, "tab count to report")
	commit := fs.String("commit", "", "PR Radar commit to report")
	behind := fs.Int("behind", -1, "commits behind to report (-1: unknown)")
	_ = fs.Parse(args)

	dir := watchDir()
	tokenFile := filepath.Join(dir, "watch-token")
	instanceID, err := readOrCreateInstanceID(filepath.Join(dir, "watch-instance"))
	if err != nil {
		return err
	}
	// The token never goes on the command line, where it would end up in the
	// shell history and in `ps`.
	token := strings.TrimSpace(os.Getenv("RADAR_HUB_TOKEN"))
	if token == "" {
		if data, err := os.ReadFile(tokenFile); err == nil {
			token = strings.TrimSpace(string(data))
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	c, _, err := websocket.Dial(dctx, *hubURL, nil)
	cancel()
	if err != nil {
		return fmt.Errorf("connect %s: %w", *hubURL, err)
	}
	defer c.CloseNow()

	hello := map[string]any{"type": "hello", "protocol": protocolVersion, "instanceId": instanceID, "tabs": *tabs}
	v := map[string]any{"commit": nil, "behind": nil}
	if *commit != "" {
		v["commit"] = *commit
	}
	if *behind >= 0 {
		v["behind"] = *behind
	}
	hello["version"] = v
	if token != "" {
		hello["hubToken"] = token
	}
	if err := writeJSON(ctx, c, hello); err != nil {
		return err
	}
	fmt.Printf("connected to %s as instance %s\n", *hubURL, shortID(instanceID))

	go func() {
		sc := bufio.NewScanner(os.Stdin)
		for sc.Scan() {
			n, err := strconv.Atoi(strings.TrimSpace(sc.Text()))
			if err != nil || n < 0 {
				fmt.Println("(type a tab count and Enter)")
				continue
			}
			_ = writeJSON(ctx, c, map[string]any{"type": "status", "tabs": n})
		}
	}()

	for {
		_, data, err := c.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				_ = c.Close(websocket.StatusNormalClosure, "bye")
				return nil
			}
			if s := websocket.CloseStatus(err); s != -1 {
				var ce websocket.CloseError
				errors.As(err, &ce)
				return fmt.Errorf("closed by the hub: %d %s", s, ce.Reason)
			}
			return err
		}
		var msg map[string]json.RawMessage
		if json.Unmarshal(data, &msg) != nil {
			continue
		}
		var typ string
		_ = json.Unmarshal(msg["type"], &typ)
		switch typ {
		case "auth_required":
			var a authRequiredMessage
			_ = json.Unmarshal(data, &a)
			fmt.Printf("\nSign in: open %s and enter %s (valid %d min)\n\n", a.VerificationURI, a.UserCode, a.ExpiresIn/60)
		case "welcome":
			var w welcomeMessage
			_ = json.Unmarshal(data, &w)
			fmt.Printf("welcome %s\n", w.Login)
			if w.HubToken != "" {
				if err := os.WriteFile(tokenFile, []byte(w.HubToken+"\n"), 0o600); err != nil {
					return err
				}
				fmt.Printf("hub token saved in %s\n", tokenFile)
			}
		case "presence":
			var p presenceMessage
			_ = json.Unmarshal(data, &p)
			printPresence(p)
		case "error":
			var e errorMessage
			_ = json.Unmarshal(data, &e)
			fmt.Printf("error %s: %s\n", e.Code, e.Message)
			if e.Code == "bad_token" || e.Code == "revoked" || e.Code == "not_member" {
				_ = os.Remove(tokenFile)
			}
		default:
			fmt.Printf("(ignored message of type %q)\n", typ)
		}
	}
}

func printPresence(p presenceMessage) {
	fmt.Printf("\n[%s] %d online\n", time.Now().Format("15:04:05"), len(p.Online))
	for _, o := range p.Online {
		fmt.Printf("  ● %-20s %d tab(s)", o.Login, o.Tabs)
		for _, r := range o.Radars {
			fmt.Printf("  [%s]", versionText(r.Commit, r.Behind))
		}
		fmt.Println()
	}
	for _, r := range p.Recent {
		t, _ := time.Parse(time.RFC3339, r.LastSeen)
		fmt.Printf("  ○ %-20s seen %s\n", r.Login, ago(t))
	}
}
