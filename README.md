# ConnectX - 4 in a Row

A real-time Connect Four style game with Go backend, WebSocket gameplay, a simple frontend, a fallback competitive bot, and optional Kafka analytics.

## Features
- Real-time matchmaking with 10s bot fallback
- Strategic bot that blocks and seeks wins
- WebSocket gameplay and reconnect within 30s before forfeit
- 30s per-turn timer with automatic random move on timeout
- In-memory active games with persistent storage for completed games (SQLite)
- Leaderboard by wins
- Optional Kafka analytics producer and consumer

## Prerequisites
- Go 1.22+
- (Optional) Kafka broker if you want analytics events

## Run the server
```bash
go run ./cmd/server
```
Then open http://localhost:8080

### Environment variables
- `KAFKA_BROKERS` (optional) - comma-separated brokers for analytics events

## Run the analytics consumer (optional)
```bash
KAFKA_BROKERS=localhost:9092 go run ./cmd/analytics
```

## Project structure
- `cmd/server` - main HTTP + WebSocket server
- `cmd/analytics` - Kafka consumer for analytics
- `internal/game` - core game logic
- `web` - static frontend
