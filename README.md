# ConnectX - 4 in a Row

A real-time Connect Four style game with Go backend, WebSocket gameplay, a simple frontend, a fallback competitive bot, and optional Kafka analytics.

## Features
- Real-time matchmaking with 10s bot fallback
- Strategic bot that blocks and seeks wins
- WebSocket gameplay and reconnect within 30s before forfeit
- 30s per-turn timer with automatic random move on timeout
- In-memory active games with leaderboard tracking (SQLite)
- Sessions end on game completion and clients reconnect to start a new game
- Optional Kafka analytics producer and consumer

## Prerequisites
- Go 1.22+
- (Optional) Kafka broker if you want analytics events

## Run the server
```bash
KAFKA_BROKERS=localhost:9092 go run ./cmd/server
```
Then open http://localhost:8080

### Environment variables
- `KAFKA_BROKERS` (optional) - comma-separated brokers for analytics events published to the `connectx-events` topic

## Run the analytics consumer (optional)
```bash
KAFKA_BROKERS=localhost:9092 go run ./cmd/analytics
```

## Kafka event stream (optional)
When `KAFKA_BROKERS` is set, the server emits JSON events (game_started, move, timeout_move, game_completed, game_reset, forfeit)
to the `connectx-events` topic. Use the analytics consumer to store or process these events for dashboards or long-term stats.

## Project structure
- `cmd/server` - app entry point
- `cmd/analytics` - Kafka consumer for analytics
- `internal/server` - server, lobby, WS, state, and game flow modules
- `internal/game` - game rules, board logic, player/status types
- `web` - static frontend (HTML, JS, CSS)
