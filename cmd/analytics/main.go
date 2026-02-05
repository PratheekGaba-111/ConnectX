package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	_ "modernc.org/sqlite"
)

type event struct {
	Type      string    `json:"type"`
	GameID    string    `json:"gameId"`
	Player1   string    `json:"player1"`
	Player2   string    `json:"player2"`
	Winner    string    `json:"winner"`
	Status    string    `json:"status"`
	StartedAt time.Time `json:"startedAt"`
	EndedAt   time.Time `json:"endedAt"`
	Timestamp time.Time `json:"timestamp"`
}

func main() {
	brokers := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if brokers == "" {
		log.Fatal("KAFKA_BROKERS required")
	}
	topic := getenv("KAFKA_TOPIC", "connectx-events")
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: strings.Split(brokers, ","),
		GroupID: "connectx-analytics",
		Topic:   topic,
	})
	defer reader.Close()

	db, err := sql.Open("sqlite", "file:analytics.db?_pragma=busy_timeout(5000)")
	if err != nil {
		log.Fatal(err)
	}
	if err := initDB(db); err != nil {
		log.Fatal(err)
	}
	if err := ensureSchema(db); err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	log.Printf("analytics consumer ready on topic %s", topic)
	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("read error: %v", err)
			continue
		}
		var ev event
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			log.Printf("unmarshal error: %v", err)
			continue
		}
		if err := storeEvent(db, ev); err != nil {
			log.Printf("store error: %v", err)
		}
		log.Printf("event %s game=%s winner=%s", ev.Type, ev.GameID, ev.Winner)
		logMetrics(db)
	}
}

func initDB(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			game_id TEXT,
			type TEXT,
			player1 TEXT,
			player2 TEXT,
			winner TEXT,
			started_at DATETIME,
			ended_at DATETIME,
			created_at DATETIME
		);
	`)
	return err
}

func ensureSchema(db *sql.DB) error {
	if err := addColumn(db, "events", "player1 TEXT"); err != nil {
		return err
	}
	if err := addColumn(db, "events", "player2 TEXT"); err != nil {
		return err
	}
	return nil
}

func addColumn(db *sql.DB, table, columnDef string) error {
	_, err := db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", table, columnDef))
	if err != nil && !strings.Contains(err.Error(), "duplicate column name") {
		return err
	}
	return nil
}

func storeEvent(db *sql.DB, ev event) error {
	_, err := db.Exec(
		"INSERT INTO events (game_id, type, player1, player2, winner, started_at, ended_at, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		ev.GameID,
		ev.Type,
		ev.Player1,
		ev.Player2,
		ev.Winner,
		ev.StartedAt,
		ev.EndedAt,
		ev.Timestamp,
	)
	return err
}

func logMetrics(db *sql.DB) {
	logAverageDuration(db)
	logTopWinners(db, 3)
	logGamesPerDay(db, 3)
	logGamesPerHour(db, 5)
	logUserMetrics(db, 5)
}

func logAverageDuration(db *sql.DB) {
	var avgDuration sql.NullFloat64
	_ = db.QueryRow(`
		SELECT AVG(strftime('%s', ended_at) - strftime('%s', started_at))
		FROM events
		WHERE type = 'game_completed'
	`).Scan(&avgDuration)
	fmt.Printf("metrics avg_duration_sec=%.2f\n", avgDuration.Float64)
}

func logTopWinners(db *sql.DB, limit int) {
	rows, err := db.Query(`
		SELECT winner, COUNT(*) AS wins
		FROM events
		WHERE type = 'game_completed' AND winner != ''
		GROUP BY winner
		ORDER BY wins DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var winner string
		var wins int
		if err := rows.Scan(&winner, &wins); err == nil {
			fmt.Printf("metrics top_winner=%s wins=%d\n", winner, wins)
		}
	}
}

func logGamesPerDay(db *sql.DB, limit int) {
	rows, err := db.Query(`
		SELECT strftime('%Y-%m-%d', started_at) AS day, COUNT(*) AS games
		FROM events
		WHERE type = 'game_completed'
		GROUP BY day
		ORDER BY day DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var day string
		var games int
		if err := rows.Scan(&day, &games); err == nil {
			fmt.Printf("metrics games_per_day day=%s games=%d\n", day, games)
		}
	}
}

func logGamesPerHour(db *sql.DB, limit int) {
	rows, err := db.Query(`
		SELECT strftime('%Y-%m-%d %H:00', started_at) AS hour, COUNT(*) AS games
		FROM events
		WHERE type = 'game_completed'
		GROUP BY hour
		ORDER BY hour DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var hour string
		var games int
		if err := rows.Scan(&hour, &games); err == nil {
			fmt.Printf("metrics games_per_hour hour=%s games=%d\n", hour, games)
		}
	}
}

func logUserMetrics(db *sql.DB, limit int) {
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
		return
	}
	defer rows.Close()
	for rows.Next() {
		var player string
		var games int
		var wins int
		var winRate float64
		if err := rows.Scan(&player, &games, &wins, &winRate); err == nil {
			fmt.Printf("metrics user=%s games=%d wins=%d win_rate=%.2f%%\n", player, games, wins, winRate)
		}
	}
}

func getenv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
