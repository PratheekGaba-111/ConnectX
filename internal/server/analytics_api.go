package server

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"time"

	_ "modernc.org/sqlite"
)

type analyticsResponse struct {
	Available      bool        `json:"available"`
	Message        string      `json:"message,omitempty"`
	AvgDurationSec float64     `json:"avgDurationSec"`
	TopWinners     []winnerRow `json:"topWinners"`
	GamesPerDay    []dayRow    `json:"gamesPerDay"`
	GamesPerHour   []hourRow   `json:"gamesPerHour"`
	UserStats      []userRow   `json:"userStats,omitempty"`
	UpdatedAt      string      `json:"updatedAt"`
}

type winnerRow struct {
	Username string `json:"username"`
	Wins     int    `json:"wins"`
}

type dayRow struct {
	Day   string `json:"day"`
	Games int    `json:"games"`
}

type hourRow struct {
	Hour  string `json:"hour"`
	Games int    `json:"games"`
}

type userRow struct {
	Username string  `json:"username"`
	Games    int     `json:"games"`
	Wins     int     `json:"wins"`
	WinRate  float64 `json:"winRate"`
}

func (s *Server) handleAnalytics(w http.ResponseWriter, r *http.Request) {
	db, err := sql.Open("sqlite", "file:analytics.db?_pragma=busy_timeout(5000)")
	if err != nil {
		http.Error(w, "analytics db unavailable", http.StatusServiceUnavailable)
		return
	}
	defer db.Close()

	exists, err := tableExists(db, "events")
	if err != nil {
		http.Error(w, "analytics db error", http.StatusServiceUnavailable)
		return
	}
	if !exists {
		writeJSON(w, analyticsResponse{
			Available: false,
			Message:   "analytics not available; run the Kafka analytics consumer first",
			UpdatedAt: time.Now().Format(time.RFC3339),
		})
		return
	}

	resp := analyticsResponse{
		Available: true,
		UpdatedAt: time.Now().Format(time.RFC3339),
	}

	resp.AvgDurationSec = queryAvgDuration(db)
	resp.TopWinners = queryTopWinners(db, 5)
	resp.GamesPerDay = queryGamesPerDay(db, 14)
	resp.GamesPerHour = queryGamesPerHour(db, 24)
	if hasPlayerColumns(db) {
		resp.UserStats = queryUserStats(db, 10)
	}

	writeJSON(w, resp)
}

func writeJSON(w http.ResponseWriter, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}

func tableExists(db *sql.DB, name string) (bool, error) {
	var found string
	err := db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, name).Scan(&found)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func hasPlayerColumns(db *sql.DB) bool {
	rows, err := db.Query(`PRAGMA table_info(events)`)
	if err != nil {
		return false
	}
	defer rows.Close()
	hasP1 := false
	hasP2 := false
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			continue
		}
		if name == "player1" {
			hasP1 = true
		}
		if name == "player2" {
			hasP2 = true
		}
	}
	return hasP1 && hasP2
}

func queryAvgDuration(db *sql.DB) float64 {
	var avg sql.NullFloat64
	_ = db.QueryRow(`
		SELECT AVG(strftime('%s', ended_at) - strftime('%s', started_at))
		FROM events
		WHERE type = 'game_completed'
	`).Scan(&avg)
	return avg.Float64
}

func queryTopWinners(db *sql.DB, limit int) []winnerRow {
	rows, err := db.Query(`
		SELECT winner, COUNT(*) AS wins
		FROM events
		WHERE type = 'game_completed' AND winner != ''
		GROUP BY winner
		ORDER BY wins DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := []winnerRow{}
	for rows.Next() {
		var row winnerRow
		if err := rows.Scan(&row.Username, &row.Wins); err == nil {
			out = append(out, row)
		}
	}
	return out
}

func queryGamesPerDay(db *sql.DB, limit int) []dayRow {
	rows, err := db.Query(`
		SELECT strftime('%Y-%m-%d', started_at) AS day, COUNT(*) AS games
		FROM events
		WHERE type = 'game_completed'
		GROUP BY day
		ORDER BY day DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := []dayRow{}
	for rows.Next() {
		var row dayRow
		if err := rows.Scan(&row.Day, &row.Games); err == nil {
			out = append(out, row)
		}
	}
	return out
}

func queryGamesPerHour(db *sql.DB, limit int) []hourRow {
	rows, err := db.Query(`
		SELECT strftime('%Y-%m-%d %H:00', started_at) AS hour, COUNT(*) AS games
		FROM events
		WHERE type = 'game_completed'
		GROUP BY hour
		ORDER BY hour DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := []hourRow{}
	for rows.Next() {
		var row hourRow
		if err := rows.Scan(&row.Hour, &row.Games); err == nil {
			out = append(out, row)
		}
	}
	return out
}

func queryUserStats(db *sql.DB, limit int) []userRow {
	rows, err := db.Query(`
		SELECT player, COUNT(*) AS games,
		       SUM(CASE WHEN winner = player THEN 1 ELSE 0 END) AS wins,
		       ROUND(100.0 * SUM(CASE WHEN winner = player THEN 1 ELSE 0 END) / COUNT(*), 2) AS win_rate
		FROM (
			SELECT player1 AS player, winner
			FROM events
			WHERE type = 'game_completed' AND player1 != ''
			UNION ALL
			SELECT player2 AS player, winner
			FROM events
			WHERE type = 'game_completed' AND player2 != ''
		)
		GROUP BY player
		ORDER BY games DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := []userRow{}
	for rows.Next() {
		var row userRow
		if err := rows.Scan(&row.Username, &row.Games, &row.Wins, &row.WinRate); err == nil {
			out = append(out, row)
		}
	}
	return out
}
