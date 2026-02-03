package server

import (
	"fmt"
	"time"

	"connectx/internal/game"

	"github.com/gorilla/websocket"
)

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
		go s.announceStatus(username, "Waiting for opponent...")
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
