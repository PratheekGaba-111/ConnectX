package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"time"

	"connectx/internal/game"

	"github.com/segmentio/kafka-go"
)

type stateMessage struct {
	Type        string      `json:"type"`
	GameID      string      `json:"gameId"`
	Board       [][]int     `json:"board"`
	Turn        string      `json:"turn"`
	Players     []string    `json:"players"`
	Status      game.Status `json:"status"`
	Winner      string      `json:"winner"`
	LastMove    *game.Move  `json:"lastMove,omitempty"`
	Message     string      `json:"message,omitempty"`
	BotGame     bool        `json:"botGame"`
	Started     string      `json:"startedAt"`
	TurnStarted string      `json:"turnStartedAt,omitempty"`
	Ended       string      `json:"endedAt,omitempty"`
}

type leaderboardEntry struct {
	Username string `json:"username"`
	Wins     int    `json:"wins"`
}

func (s *Server) sendState(g *game.Game, message string, botGame bool) {
	players := []string{}
	for _, p := range g.Players {
		if p != nil {
			players = append(players, p.Username)
		}
	}
	turn := ""
	if g.Status == game.StatusActive {
		turn = g.CurrentPlayer().Username
	}
	winner := ""
	if g.Status == game.StatusFinished && g.WinnerID != 0 {
		for _, p := range g.Players {
			if p != nil && p.ID == g.WinnerID {
				winner = p.Username
			}
		}
	}
	state := stateMessage{
		Type:     "state",
		GameID:   g.ID,
		Board:    g.CopyBoard(),
		Turn:     turn,
		Players:  players,
		Status:   g.Status,
		Winner:   winner,
		LastMove: g.LastMove,
		Message:  message,
		BotGame:  botGame,
		Started:  g.StartedAt.Format(time.RFC3339),
	}
	if !g.TurnStartedAt.IsZero() {
		state.TurnStarted = g.TurnStartedAt.Format(time.RFC3339)
	}
	if !g.EndedAt.IsZero() {
		state.Ended = g.EndedAt.Format(time.RFC3339)
	}
	for _, p := range g.Players {
		if p == nil || !p.Online || p.IsBot {
			continue
		}
		if conn, ok := s.connections[p.Username]; ok && conn != nil {
			_ = conn.WriteJSON(state)
		}
	}
}

func (s *Server) sendMessage(username, msgType, message string) {
	if conn, ok := s.connections[username]; ok && conn != nil {
		_ = conn.WriteJSON(map[string]string{"type": msgType, "message": message})
	}
}

func initDB(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS games (
			id TEXT PRIMARY KEY,
			player1 TEXT,
			player2 TEXT,
			winner TEXT,
			started_at DATETIME,
			ended_at DATETIME
		);
	`)
	return err
}

func (s *Server) saveGame(g *game.Game, winner string) error {
	_, err := s.db.Exec(
		"INSERT OR REPLACE INTO games (id, player1, player2, winner, started_at, ended_at) VALUES (?, ?, ?, ?, ?, ?)",
		g.ID,
		g.Players[0].Username,
		g.Players[1].Username,
		winner,
		g.StartedAt,
		g.EndedAt,
	)
	return err
}

func (s *Server) emitEvent(eventType string, g *game.Game, actor string) {
	if s.kafkaWriter == nil {
		return
	}
	payload := map[string]interface{}{
		"type":      eventType,
		"gameId":    g.ID,
		"player1":   g.Players[0].Username,
		"player2":   g.Players[1].Username,
		"winner":    actor,
		"status":    g.Status,
		"startedAt": g.StartedAt,
		"endedAt":   g.EndedAt,
		"timestamp": time.Now(),
	}
	data, _ := json.Marshal(payload)
	_ = s.kafkaWriter.WriteMessages(context.Background(), kafka.Message{Value: data})
}

func newKafkaWriter() *kafka.Writer {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		return nil
	}
	return &kafka.Writer{
		Addr:     kafka.TCP(splitAndTrim(brokers)...),
		Topic:    "connectx-events",
		Balancer: &kafka.LeastBytes{},
	}
}

func splitAndTrim(value string) []string {
	parts := []string{}
	for _, part := range strings.Split(value, ",") {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			parts = append(parts, trimmed)
		}
	}
	return parts
}

func (s *Server) handleLeaderboard(w http.ResponseWriter, r *http.Request) {
	s.leaderboardMux.Lock()
	defer s.leaderboardMux.Unlock()
	rows, err := s.db.Query(`
		SELECT winner, COUNT(*) as wins
		FROM games
		WHERE winner IS NOT NULL AND winner != ''
		GROUP BY winner
		ORDER BY wins DESC
		LIMIT 10
	`)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	leaders := []leaderboardEntry{}
	for rows.Next() {
		var entry leaderboardEntry
		if err := rows.Scan(&entry.Username, &entry.Wins); err == nil {
			leaders = append(leaders, entry)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(leaders)
}

func (s *Server) announceStatus(username, message string) {
	if conn, ok := s.connections[username]; ok && conn != nil {
		_ = conn.WriteJSON(map[string]string{"type": "status", "message": message})
	}
}
