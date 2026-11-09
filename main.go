package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/coder/websocket"
)

const usage = `radar-hub — presence hub for PR Radar

  radar-hub serve              run the hub
  radar-hub status [-json]     who is online, versions, tokens (asks the running hub)
  radar-hub revoke <login>     revoke a login and refuse its next sign-in
  radar-hub unrevoke <login>   allow a revoked login to sign in again
  radar-hub invite <login>     issue a hub token by hand (someone outside the org)
  radar-hub watch [-tabs N]    act as a radar and print the presence it receives

Configuration comes from the environment, see .env.example.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	cfg, err := configFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	cmd, args := os.Args[1], os.Args[2:]
	switch cmd {
	case "serve":
		err = serve(cfg)
	case "status":
		fs := flag.NewFlagSet("status", flag.ExitOnError)
		asJSON := fs.Bool("json", false, "print the raw report")
		_ = fs.Parse(args)
		err = cmdStatus(cfg, *asJSON)
	case "revoke", "unrevoke", "invite":
		if len(args) != 1 {
			fmt.Fprintf(os.Stderr, "usage: radar-hub %s <login>\n", cmd)
			os.Exit(2)
		}
		err = cmdAdminAction(cfg, cmd, args[0])
	case "watch":
		err = cmdWatch(args)
	case "help", "-h", "--help":
		fmt.Print(usage)
	default:
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "radar-hub:", err)
		os.Exit(1)
	}
}

func (h *Hub) publicHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", h.ServeWS)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true, "tokenOk": h.gh.Status(h.cfg.TokenWarning).OK})
	})
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintln(w, "PR Radar Hub. See https://github.com/matthv/pr-radar-hub")
	})
	return mux
}

func serve(cfg Config) error {
	logger := log.New(os.Stderr, "", 0)
	if cfg.ClientID == "" {
		return errors.New("HUB_GITHUB_CLIENT_ID is required")
	}
	store, err := openStore(cfg.DataDir, time.Now)
	if err != nil {
		return fmt.Errorf("data: %w", err)
	}
	gh := newGitHub(cfg, time.Now)
	h := newHub(cfg, store, gh, logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go h.broadcaster(ctx)
	go h.revalidator(ctx)
	go func() {
		if err := gh.CheckToken(ctx); err != nil {
			logger.Printf("org token: %v", err)
		}
	}()

	adminL, err := listenAdmin(cfg.adminSocket())
	if err != nil {
		return fmt.Errorf("admin socket: %w", err)
	}
	admin := &http.Server{Handler: h.adminHandler()}
	go func() { _ = admin.Serve(adminL) }()

	public := &http.Server{
		Addr:              cfg.Addr,
		Handler:           h.publicHandler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	errc := make(chan error, 1)
	go func() { errc <- public.ListenAndServe() }()
	logger.Printf("listening on %s, org %s", cfg.Addr, cfg.Org)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	logger.Printf("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h.touchOnline()
	h.closeAll(websocket.StatusGoingAway, "hub restarting")
	_ = public.Shutdown(sctx)
	_ = admin.Shutdown(sctx)
	_ = os.Remove(cfg.adminSocket())
	return nil
}
