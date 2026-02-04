package server

import (
	"database/sql"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"connectx/internal/game"

	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
	_ "modernc.org/sqlite"
)

type Server struct {
	mu              sync.Mutex
	waiting         *game.Player
	waitingTimer    *time.Timer
	games           map[string]*game.Game
	connections     map[string]*websocket.Conn
	disconnectAt    map[string]time.Time
	forfeitTimers   map[string]*time.Timer
	turnTimers      map[string]*time.Timer
	rematchRequests map[string]map[string]bool
	players         map[string]*game.Player
	db              *sql.DB
	kafkaWriter     *kafka.Writer
	leaderboardMux  sync.Mutex
}

func New() (*Server, func(), error) {
	rand.Seed(time.Now().UnixNano())
	db, err := sql.Open("sqlite", "file:connectx.db?_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, nil, err
	}
	if err := initDB(db); err != nil {
		return nil, nil, err
	}
	writer := newKafkaWriter()
	srv := &Server{
		games:           make(map[string]*game.Game),
		connections:     make(map[string]*websocket.Conn),
		disconnectAt:    make(map[string]time.Time),
		forfeitTimers:   make(map[string]*time.Timer),
		turnTimers:      make(map[string]*time.Timer),
		rematchRequests: make(map[string]map[string]bool),
		players:         make(map[string]*game.Player),
		db:              db,
		kafkaWriter:     writer,
	}
	cleanup := func() {
		if writer != nil {
			_ = writer.Close()
		}
		_ = db.Close()
	}
	return srv, cleanup, nil
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleWS)
	mux.HandleFunc("/api/leaderboard", s.handleLeaderboard)
	mux.Handle("/", http.FileServer(http.Dir("./web")))
	return mux
}
