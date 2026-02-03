package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"connectx/internal/game"

	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
	_ "modernc.org/sqlite"
)

type Server struct {
	mu             sync.Mutex
	waiting        *game.Player
	waitingTimer   *time.Timer
	games          map[string]*game.Game
	connections    map[string]*websocket.Conn
	disconnectAt   map[string]time.Time
	forfeitTimers  map[string]*time.Timer
	turnTimers     map[string]*time.Timer
	db             *sql.DB
	kafkaWriter    *kafka.Writer
	leaderboardMux sync.Mutex
}

type wsMessage struct {
	Type     string `json:"type"`
	Column   int    `json:"column,omitempty"`
	Username string `json:"username,omitempty"`
	GameID   string `json:"gameId,omitempty"`
}

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

func main() {
	rand.Seed(time.Now().UnixNano())
	db, err := sql.Open("sqlite", "file:connectx.db?_pragma=busy_timeout(5000)")
	if err != nil {
		log.Fatal(err)
	}
	if err := initDB(db); err != nil {
		log.Fatal(err)
	}
	kafkaWriter := newKafkaWriter()
	server := &Server{
		games:         make(map[string]*game.Game),
		connections:   make(map[string]*websocket.Conn),
		disconnectAt:  make(map[string]time.Time),
		forfeitTimers: make(map[string]*time.Timer),
		turnTimers:    make(map[string]*time.Timer),
		db:            db,
		kafkaWriter:   kafkaWriter,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", server.handleWS)
	mux.HandleFunc("/api/leaderboard", server.handleLeaderboard)
	mux.Handle("/", http.FileServer(http.Dir("./web")))

	serverHTTP := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		log.Printf("server listening on %s", serverHTTP.Addr)
		if err := serverHTTP.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := serverHTTP.Shutdown(ctx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
	if kafkaWriter != nil {
		_ = kafkaWriter.Close()
	}
	_ = db.Close()
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

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("upgrade error: %v", err)
		return
	}
	defer conn.Close()

	var join wsMessage
	if err := conn.ReadJSON(&join); err != nil {
		log.Printf("read join error: %v", err)
		return
	}
	username := strings.TrimSpace(join.Username)
	if username == "" {
		_ = conn.WriteJSON(map[string]string{"type": "error", "message": "username required"})
		return
	}

	player, gameInstance, isRejoin := s.attachPlayer(username, conn)
	if gameInstance != nil {
		s.sendState(gameInstance, "", false)
	}
	if isRejoin {
		log.Printf("%s rejoined game %s", username, gameInstance.ID)
	}

	for {
		var msg wsMessage
		if err := conn.ReadJSON(&msg); err != nil {
			s.handleDisconnect(player.Username)
			return
		}
		if msg.Type == "move" {
			s.handleMove(gameInstance, player, msg.Column)
		}
		if msg.Type == "reset" {
			s.handleReset(gameInstance, player)
		}
	}
}

func (s *Server) attachPlayer(username string, conn *websocket.Conn) (*game.Player, *game.Game, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existingConn, ok := s.connections[username]; ok {
		if existingConn != nil {
			_ = conn.WriteJSON(map[string]string{"type": "error", "message": "username already connected"})
			return nil, nil, false
		}
	}

	for _, g := range s.games {
		for _, p := range g.Players {
			if p != nil && p.Username == username {
				p.Online = true
				p.ConnID = username
				s.connections[username] = conn
				delete(s.disconnectAt, username)
				if timer, ok := s.forfeitTimers[username]; ok {
					timer.Stop()
					delete(s.forfeitTimers, username)
				}
				return p, g, true
			}
		}
	}

	player := &game.Player{Username: username, ID: 1, Online: true, ConnID: username}
	s.connections[username] = conn

	if s.waiting == nil {
		s.waiting = player
		player.ID = 1
		waitingGame := game.NewWaitingGame(randomID(), player)
		s.games[waitingGame.ID] = waitingGame
		s.waitingTimer = time.AfterFunc(10*time.Second, func() {
			s.startBotGame(username)
		})
		go func() {
			_ = conn.WriteJSON(map[string]string{"type": "status", "message": "Waiting for opponent..."})
		}()
		return player, waitingGame, false
	}

	opponent := s.waiting
	if s.waitingTimer != nil {
		s.waitingTimer.Stop()
		s.waitingTimer = nil
	}
	s.waiting = nil

	player.ID = 2
	opponent.ID = 1
	gameInstance := s.findGameByPlayer(opponent.Username)
	if gameInstance == nil {
		gameInstance = game.NewGame(randomID(), opponent, player)
		s.games[gameInstance.ID] = gameInstance
	} else {
		gameInstance.Players[1] = player
		gameInstance.Status = game.StatusActive
		gameInstance.StartedAt = time.Now()
		gameInstance.TurnStartedAt = time.Now()
	}
	go s.emitEvent("game_started", gameInstance, "")
	s.startTurnTimer(gameInstance)
	return player, gameInstance, false
}

func (s *Server) startBotGame(username string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.waiting == nil || s.waiting.Username != username {
		return
	}
	player := s.waiting
	s.waiting = nil

	bot := &game.Player{Username: "Bot", ID: 2, IsBot: true, Online: true}
	gameInstance := s.findGameByPlayer(player.Username)
	if gameInstance == nil {
		gameInstance = game.NewGame(randomID(), player, bot)
		s.games[gameInstance.ID] = gameInstance
	} else {
		gameInstance.Players[1] = bot
		gameInstance.Status = game.StatusActive
		gameInstance.StartedAt = time.Now()
		gameInstance.TurnStartedAt = time.Now()
	}
	go s.emitEvent("game_started", gameInstance, "bot")
	s.sendState(gameInstance, "Bot opponent joined", true)
	s.startTurnTimer(gameInstance)
}

func (s *Server) handleMove(g *game.Game, player *game.Player, col int) {
	if g.Status != game.StatusActive {
		return
	}
	if g.CurrentPlayer().Username != player.Username {
		s.sendMessage(player.Username, "error", "Not your turn")
		return
	}
	move, err := g.DropDisc(col)
	if err != nil {
		s.sendMessage(player.Username, "error", err.Error())
		return
	}
	go s.emitEvent("move", g, player.Username)
	s.sendState(g, "", false)
	s.startTurnTimer(g)
	if g.Status == game.StatusFinished {
		s.finalizeGame(g)
		return
	}
	if g.CurrentPlayer().IsBot {
		s.handleBotTurn(g)
	}
	if move != nil {
		_ = move
	}
}

func (s *Server) handleBotTurn(g *game.Game) {
	bot := g.CurrentPlayer()
	opponent := g.Players[(g.Turn+1)%2]
	col := g.FindStrategicMove(bot.ID, opponent.ID)
	_, err := g.DropDisc(col)
	if err != nil {
		return
	}
	go s.emitEvent("move", g, bot.Username)
	s.sendState(g, "", true)
	s.startTurnTimer(g)
	if g.Status == game.StatusFinished {
		s.finalizeGame(g)
	}
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

func (s *Server) handleDisconnect(username string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connections[username] = nil
	s.disconnectAt[username] = time.Now()

	for _, g := range s.games {
		for _, p := range g.Players {
			if p != nil && p.Username == username {
				p.Online = false
				s.startForfeitTimer(g, p)
			}
		}
	}
}

func (s *Server) startForfeitTimer(g *game.Game, p *game.Player) {
	if g.Status != game.StatusActive {
		return
	}
	if timer, ok := s.forfeitTimers[p.Username]; ok {
		timer.Stop()
	}
	s.forfeitTimers[p.Username] = time.AfterFunc(30*time.Second, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if p.Online {
			return
		}
		g.Status = game.StatusFinished
		g.EndedAt = time.Now()
		winner := g.Players[(p.ID)%2]
		if winner != nil {
			g.WinnerID = winner.ID
		}
		go s.emitEvent("forfeit", g, p.Username)
		s.finalizeGame(g)
		s.sendState(g, fmt.Sprintf("%s forfeited", p.Username), false)
	})
}

func (s *Server) startTurnTimer(g *game.Game) {
	if g.Status != game.StatusActive {
		return
	}
	if timer, ok := s.turnTimers[g.ID]; ok {
		timer.Stop()
	}
	s.turnTimers[g.ID] = time.AfterFunc(30*time.Second, func() {
		s.handleTurnTimeout(g.ID)
	})
}

func (s *Server) stopTurnTimer(gameID string) {
	if timer, ok := s.turnTimers[gameID]; ok {
		timer.Stop()
		delete(s.turnTimers, gameID)
	}
}

func (s *Server) handleTurnTimeout(gameID string) {
	s.mu.Lock()
	gameInstance, ok := s.games[gameID]
	s.mu.Unlock()
	if !ok || gameInstance.Status != game.StatusActive {
		return
	}
	current := gameInstance.CurrentPlayer()
	if current == nil || !current.Online || current.IsBot {
		return
	}
	col := gameInstance.RandomValidColumn()
	if col == -1 {
		return
	}
	_, err := gameInstance.DropDisc(col)
	if err != nil {
		return
	}
	go s.emitEvent("timeout_move", gameInstance, current.Username)
	s.sendState(gameInstance, fmt.Sprintf("%s timed out. Random move played.", current.Username), false)
	s.startTurnTimer(gameInstance)
	if gameInstance.Status == game.StatusFinished {
		s.finalizeGame(gameInstance)
	}
}

func (s *Server) finalizeGame(g *game.Game) {
	s.stopTurnTimer(g.ID)
	winner := ""
	if g.WinnerID != 0 {
		for _, p := range g.Players {
			if p != nil && p.ID == g.WinnerID {
				winner = p.Username
			}
		}
	}
	if err := s.saveGame(g, winner); err != nil {
		log.Printf("save game error: %v", err)
	}
	go s.emitEvent("game_completed", g, winner)
}

func (s *Server) handleReset(g *game.Game, player *game.Player) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if g != nil {
		if g.Status == game.StatusActive {
			g.Status = game.StatusFinished
			g.EndedAt = time.Now()
			opponent := g.Players[(player.ID)%2]
			if opponent != nil {
				g.WinnerID = opponent.ID
			}
		}
		s.sendState(g, fmt.Sprintf("%s started a new match.", player.Username), false)
		s.stopTurnTimer(g.ID)
		go s.emitEvent("game_reset", g, player.Username)
		if err := s.saveGame(g, ""); err != nil {
			log.Printf("save game error: %v", err)
		}
	}
	player.Online = true
	if s.waiting == nil {
		s.waiting = player
		player.ID = 1
		waitingGame := game.NewWaitingGame(randomID(), player)
		s.games[waitingGame.ID] = waitingGame
		s.waitingTimer = time.AfterFunc(10*time.Second, func() {
			s.startBotGame(player.Username)
		})
		if conn, ok := s.connections[player.Username]; ok && conn != nil {
			_ = conn.WriteJSON(map[string]string{"type": "status", "message": "Looking for a new opponent..."})
		}
		return
	}
	opponent := s.waiting
	if s.waitingTimer != nil {
		s.waitingTimer.Stop()
		s.waitingTimer = nil
	}
	s.waiting = nil
	player.ID = 2
	opponent.ID = 1
	newGame := s.findGameByPlayer(opponent.Username)
	if newGame == nil {
		newGame = game.NewGame(randomID(), opponent, player)
		s.games[newGame.ID] = newGame
	} else {
		newGame.Players[1] = player
		newGame.Status = game.StatusActive
		newGame.StartedAt = time.Now()
		newGame.TurnStartedAt = time.Now()
	}
	go s.emitEvent("game_started", newGame, "")
	s.sendState(newGame, "New match started!", false)
	s.startTurnTimer(newGame)
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

func (s *Server) findGameByPlayer(username string) *game.Game {
	for _, g := range s.games {
		for _, p := range g.Players {
			if p != nil && p.Username == username {
				return g
			}
		}
	}
	return nil
}

func randomID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
