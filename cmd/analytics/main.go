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
			winner TEXT,
			started_at DATETIME,
			ended_at DATETIME,
			created_at DATETIME
		);
	`)
	return err
}

func storeEvent(db *sql.DB, ev event) error {
	_, err := db.Exec(
		"INSERT INTO events (game_id, type, winner, started_at, ended_at, created_at) VALUES (?, ?, ?, ?, ?, ?)",
		ev.GameID,
		ev.Type,
		ev.Winner,
		ev.StartedAt,
		ev.EndedAt,
		ev.Timestamp,
	)
	return err
}

func logMetrics(db *sql.DB) {
	var avgDuration sql.NullFloat64
	_ = db.QueryRow(`
		SELECT AVG(strftime('%s', ended_at) - strftime('%s', started_at))
		FROM events
		WHERE type = 'game_completed'
	`).Scan(&avgDuration)

	var topWinner string
	var wins int
	_ = db.QueryRow(`
		SELECT winner, COUNT(*)
		FROM events
		WHERE type = 'game_completed' AND winner != ''
		GROUP BY winner
		ORDER BY COUNT(*) DESC
		LIMIT 1
	`).Scan(&topWinner, &wins)

	fmt.Printf("metrics avg_duration_sec=%.2f top_winner=%s wins=%d\n", avgDuration.Float64, topWinner, wins)
}

func getenv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
