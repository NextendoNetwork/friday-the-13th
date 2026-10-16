package main

// backend.go serves IllFonic's Friday the 13th REST backend (https://nswitch.f13.illfonic.com).
// Traefik terminates TLS and forwards plain HTTP here. The game calls it right after NEX
// Register; "Database login failure!" means POST /v0/login/nintendo did not succeed.

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	backendPort = envOrInt("BACKEND_PORT", 8470)
	// The game reaches nswitch.f13.illfonic.com directly on per-service ports (endpoint table at
	// main 0x531980): 9130 login/account, 9110 validate/heartbeat/jason, 9111 aes256,
	// 9120 report, 9140 stats/inventory/matchmaking status, 9150 news, 3030 matchmaking queue.
	backendTLSPorts = []int{3030, 9110, 9111, 9120, 9130, 9140, 9150}
)

func startBackend() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", handleBackend)
	newServer := func(port int) *http.Server {
		return &http.Server{
			Addr:              fmt.Sprintf(":%d", port),
			Handler:           mux,
			ReadHeaderTimeout: 10 * time.Second,
		}
	}
	for _, port := range backendTLSPorts {
		go func(port int) {
			fmt.Printf("[Friday13 Backend] listening HTTPS :%d (game direct)\n", port)
			if err := newServer(port).ListenAndServeTLS(certFile, keyFile); err != nil {
				fmt.Printf("[Friday13 Backend] TLS :%d stopped: %v\n", port, err)
			}
		}(port)
	}
	fmt.Printf("[Friday13 Backend] listening HTTP :%d (nswitch.f13.illfonic.com via Traefik)\n", backendPort)
	if err := newServer(backendPort).ListenAndServe(); err != nil {
		fmt.Printf("[Friday13 Backend] stopped: %v\n", err)
	}
}

func handleBackend(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	var hdr []string
	for k, v := range r.Header {
		hdr = append(hdr, k+"="+strings.Join(v, ","))
	}
	fmt.Printf("[Friday13 Backend] %s %s host=%s from=%s session=%s body=%q headers=[%s]\n",
		r.Method, r.URL.RequestURI(), r.Host, r.RemoteAddr, r.Header.Get("Session-Token"), body,
		strings.Join(hdr, "; "))

	resp := map[string]any{}
	switch r.URL.Path {
	case "/v0/login/nintendo":
		// The game requires 200, Content-Type exactly application/json, and string UserId +
		// SessionToken; a 401 whose Error contains "account is locked" is the locked case.
		nintendoID := r.URL.Query().Get("NintendoID")
		userID := userIDFor(nintendoID)
		token := randomToken()
		sessionsMu.Lock()
		sessions[token] = userID
		sessionsMu.Unlock()
		resp = map[string]any{
			"UserId":       userID,
			"SessionToken": token,
		}
	case "/v0/account":
		resp = accountResponse(withProfile(accountFor(r), nil))
	case "/v0/stats":
		resp = statsResponse(withProfile(accountFor(r), nil))
	case "/v0/achievements":
		resp = achievementsResponse(withProfile(accountFor(r), nil))
	case "/v0/achievements/unlock":
		value := strings.TrimSpace(string(body))
		if value == "" {
			value = "1"
		}
		resp = achievementsResponse(unlockAward(accountFor(r), r.URL.Query().Get("UnlockID"), value))
	case "/v0/stats/matches/report":
		recordMatchReport(accountFor(r), body)
	case "/v0/matchmaking/status":
		// Parsed at main 0x3ae3a8: Statuses[{AccountID, MatchmakingStatus "OK"|"Banned", MatchmakingBanSeconds}].
		resp = map[string]any{"Statuses": []any{map[string]any{
			"AccountID":             accountFor(r),
			"MatchmakingStatus":     "OK",
			"MatchmakingBanSeconds": 0,
		}}}
	case "/v1/experience":
		// POST ?AccountID=%s&Exp=%d adds XP; the reply is parsed at main 0x534368.
		exp, _ := strconv.ParseInt(r.URL.Query().Get("Exp"), 10, 64)
		p := addExperience(accountFor(r), exp)
		ep := experienceProfile(p)
		resp = map[string]any{
			"TotalExperience":        ep["TotalExperience"],
			"PlayerLevel":            ep["Level"],
			"XPAmountToCurrentLevel": ep["ToCurrentLevel"],
			"XPAmountToNextLevel":    ep["ToNextLevel"],
		}
	case "/v0/data/modifiers":
		// Parsed at main 0x5ced20: GetNumberField("ExperienceMultiplier"); a missing field is 0x XP.
		resp = map[string]any{"ExperienceMultiplier": 1}
	case "/v0/news":
		// Parsed at main 0x5308c8: TotalCount + NewsItems[{NewsItemId, UnixTime, Headline, Content}].
		resp = newsResponse()
	}
	out, _ := json.Marshal(resp)
	fmt.Printf("[Friday13 Backend] -> 200 %s\n", out)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(out)
}

type newsItem struct {
	ID       int
	Unix     int64
	Headline string
	Content  string
}

var newsItems = []newsItem{
	{
		ID:       1,
		Unix:     time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC).Unix(),
		Headline: "Camp Crystal Lake is open again",
		Content:  "Friday the 13th: The Game is back online on Nextendo Network. Find a match, survive the night, and watch out for Jason.",
	},
}

func newsResponse() map[string]any {
	items := make([]map[string]any, 0, len(newsItems))
	for _, n := range newsItems {
		items = append(items, map[string]any{
			"NewsItemId": n.ID,
			"UnixTime":   n.Unix,
			"Headline":   n.Headline,
			"Content":    n.Content,
		})
	}
	return map[string]any{"TotalCount": len(items), "NewsItems": items}
}

var (
	sessionsMu sync.Mutex
	sessions   = map[string]string{} // SessionToken -> UserId
)

// accountFor identifies the caller: the AccountID query parameter, else its Session-Token.
func accountFor(r *http.Request) string {
	if id := strings.ToLower(r.URL.Query().Get("AccountID")); validID.MatchString(id) {
		return id
	}
	sessionsMu.Lock()
	defer sessionsMu.Unlock()
	if id, ok := sessions[r.Header.Get("Session-Token")]; ok {
		return id
	}
	return "unknown"
}

// userIDFor gives each Nintendo ID a stable GUID-shaped user id.
func userIDFor(nintendoID string) string {
	sum := sha256.Sum256([]byte("friday13-user:" + nintendoID))
	h := hex.EncodeToString(sum[:16])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

func randomToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
