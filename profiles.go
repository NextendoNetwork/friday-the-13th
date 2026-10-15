package main

// profiles.go keeps each player's backend profile (XP, CP, achievements, stats) on disk.
// Field names and types come from the game's parsers in main (1.08.00):
//   0x5dd744 Account{ExperienceProfile, WalletProfile, InventoryProfile}
//   0x5e01e8 AchievementsProfile{Badges[{ClassName,AwardCount}], Awards[{AwardID,Value}]}
//   0x63a834 StatsProfile{Jason{MatchesPlayed,CounselorsKilled,Deaths,KillsPerformed[],Models[]},
//                         Counselor{MatchesPlayed,Models[]}}

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

var stateDir = envOr("FRIDAY13_STATE_DIR", "state")

type award struct {
	AwardID string `json:"AwardID"`
	Value   string `json:"Value"`
}

type badge struct {
	ClassName  string `json:"ClassName"`
	AwardCount int    `json:"AwardCount"`
}

type roleStats struct {
	MatchesPlayed    int   `json:"MatchesPlayed"`
	CounselorsKilled int   `json:"CounselorsKilled,omitempty"`
	Deaths           int   `json:"Deaths,omitempty"`
	KillsPerformed   []any `json:"KillsPerformed,omitempty"`
	Models           []any `json:"Models"`
}

type profile struct {
	AccountID           string            `json:"AccountID"`
	TotalExperience     int64             `json:"TotalExperience"`
	CustomizationPoints int64             `json:"CustomizationPoints"`
	Awards              []award           `json:"Awards"`
	Badges              []badge           `json:"Badges"`
	Jason               roleStats         `json:"Jason"`
	Counselor           roleStats         `json:"Counselor"`
	MatchReports        []json.RawMessage `json:"MatchReports,omitempty"`
}

var (
	profilesMu sync.Mutex
	validID    = regexp.MustCompile(`^[0-9a-f-]{8,64}$`)
)

func profilePath(id string) string { return filepath.Join(stateDir, id+".json") }

// loadProfile returns the stored profile for id; the caller holds profilesMu.
func loadProfile(id string) *profile {
	p := &profile{AccountID: id}
	if b, err := os.ReadFile(profilePath(id)); err == nil {
		_ = json.Unmarshal(b, p)
	}
	if p.Jason.Models == nil {
		p.Jason.Models = []any{}
	}
	if p.Counselor.Models == nil {
		p.Counselor.Models = []any{}
	}
	return p
}

// saveProfile writes p atomically; the caller holds profilesMu.
func saveProfile(p *profile) {
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		fmt.Printf("[Friday13 Profile] mkdir: %v\n", err)
		return
	}
	b, _ := json.MarshalIndent(p, "", "  ")
	tmp := profilePath(p.AccountID) + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err == nil {
		_ = os.Rename(tmp, profilePath(p.AccountID))
	}
}

func withProfile(id string, fn func(p *profile) bool) *profile {
	profilesMu.Lock()
	defer profilesMu.Unlock()
	p := loadProfile(id)
	// An unidentified caller gets a blank profile and never writes a shared file.
	if fn != nil && fn(p) && id != "unknown" {
		saveProfile(p)
	}
	return p
}

// Level curve: each level costs 1000 XP more than the previous one, from 1000 XP for level 2.
func levelFor(total int64) (level int, toCurrent, toNext int64) {
	level, start, step := 1, int64(0), int64(1000)
	for total >= start+step && level < 150 {
		start += step
		step += 1000
		level++
	}
	return level, start, start + step
}

func experienceProfile(p *profile) map[string]any {
	level, cur, next := levelFor(p.TotalExperience)
	return map[string]any{
		"TotalExperience": p.TotalExperience,
		"Level":           level,
		"ToCurrentLevel":  cur,
		"ToNextLevel":     next,
	}
}

func accountResponse(p *profile) map[string]any {
	return map[string]any{
		"Account": map[string]any{
			"AccountID":         p.AccountID,
			"ExperienceProfile": experienceProfile(p),
			"WalletProfile":     map[string]any{"CustomizationPoints": p.CustomizationPoints},
			"InventoryProfile": map[string]any{
				"Jasons":                 []any{},
				"JasonKills":             []any{},
				"Counselors":             []any{},
				"CounselorPerkInstances": []any{},
			},
		},
	}
}

func achievementsResponse(p *profile) map[string]any {
	awards, badges := p.Awards, p.Badges
	if awards == nil {
		awards = []award{}
	}
	if badges == nil {
		badges = []badge{}
	}
	return map[string]any{"AchievementsProfile": map[string]any{"Badges": badges, "Awards": awards}}
}

func statsResponse(p *profile) map[string]any {
	return map[string]any{"StatsProfile": map[string]any{"Jason": p.Jason, "Counselor": p.Counselor}}
}

func addExperience(id string, exp int64) *profile {
	return withProfile(id, func(p *profile) bool {
		if exp <= 0 {
			return false
		}
		p.TotalExperience += exp
		p.CustomizationPoints += exp
		fmt.Printf("[Friday13 Profile] %s +%d XP -> %d XP, %d CP\n", id, exp, p.TotalExperience, p.CustomizationPoints)
		return true
	})
}

func unlockAward(id, awardID, value string) *profile {
	return withProfile(id, func(p *profile) bool {
		for i := range p.Awards {
			if p.Awards[i].AwardID == awardID {
				p.Awards[i].Value = value
				return true
			}
		}
		p.Awards = append(p.Awards, award{AwardID: awardID, Value: value})
		return true
	})
}

func recordMatchReport(id string, body []byte) {
	withProfile(id, func(p *profile) bool {
		entry, _ := json.Marshal(map[string]any{"at": time.Now().Unix(), "report": json.RawMessage(validJSONOr(body))})
		p.MatchReports = append(p.MatchReports, entry)
		if len(p.MatchReports) > 50 {
			p.MatchReports = p.MatchReports[len(p.MatchReports)-50:]
		}
		return true
	})
}

func validJSONOr(b []byte) []byte {
	if json.Valid(b) {
		return b
	}
	q, _ := json.Marshal(string(b))
	return q
}
