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
			_ = existingConn.WriteJSON(map[string]string{"type": "status", "message": "Reconnected from another session."})
			_ = existingConn.Close()
			s.connections[username] = nil
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

	player, ok := s.players[username]
	if !ok {
		player = &game.Player{Username: username, ID: 1, Online: true, ConnID: username}
		s.players[username] = player
	} else {
		player.Online = true
		player.ConnID = username
	}
	s.connections[username] = conn
	go s.announceStatus(username, "Logged in. Click New Game to find an opponent.")
	return player, nil, false
}

func (s *Server) handleNewGame(player *game.Player) {
	if player == nil {
		return
	}
	var notifyOpponent string
	var oldGame *game.Game
	var shouldFinalize bool
	s.mu.Lock()
	for _, g := range s.games {
		for _, p := range g.Players {
			if p != nil && p.Username == player.Username {
				oldGame = g
				break
			}
		}
		if oldGame != nil {
			break
		}
	}
	if oldGame != nil {
		delete(s.rematchRequests, oldGame.ID)
		if timer, ok := s.rematchTimers[oldGame.ID]; ok {
			timer.Stop()
			delete(s.rematchTimers, oldGame.ID)
		}
		delete(s.rematchRequesters, oldGame.ID)
		s.stopTurnTimer(oldGame.ID)
		opponent := oldGame.Players[(player.ID)%2]
		if opponent != nil && !opponent.IsBot {
			notifyOpponent = opponent.Username
		}
		if oldGame.Status == game.StatusActive {
			oldGame.Status = game.StatusFinished
			oldGame.EndedAt = time.Now()
			if opponent != nil {
				oldGame.WinnerID = opponent.ID
			}
			shouldFinalize = true
		}
		delete(s.games, oldGame.ID)
	}

	var gameInstance *game.Game
	if s.waiting == nil {
		s.waiting = player
		player.ID = 1
		waitingGame := game.NewWaitingGame(randomID(), player)
		s.games[waitingGame.ID] = waitingGame
		s.waitingTimer = time.AfterFunc(10*time.Second, func() {
			s.startBotGame(player.Username)
		})
		gameInstance = waitingGame
	} else {
		opponent := s.waiting
		if s.waitingTimer != nil {
			s.waitingTimer.Stop()
			s.waitingTimer = nil
		}
		s.waiting = nil
		player.ID = 2
		opponent.ID = 1
		gameInstance = game.NewGame(randomID(), opponent, player)
		s.games[gameInstance.ID] = gameInstance
	}
	s.mu.Unlock()

	if notifyOpponent != "" {
		s.sendMessage(notifyOpponent, "status", "Opponent left. Start a new game to keep playing.")
	}
	if shouldFinalize {
		s.finalizeGame(oldGame)
	}
	if gameInstance == nil {
		return
	}
	if gameInstance.Status == game.StatusWaiting {
		s.sendState(gameInstance, "", false)
		s.announceStatus(player.Username, "Waiting for opponent...")
		return
	}
	go s.emitEvent("game_started", gameInstance, "")
	botGame := gameInstance.Players[0].IsBot || gameInstance.Players[1].IsBot
	s.sendState(gameInstance, "New match started!", botGame)
	s.startTurnTimer(gameInstance)
}

func (s *Server) handleLogout(player *game.Player) {
	if player == nil {
		return
	}
	username := player.Username
	var conn *websocket.Conn
	var waitingCleared bool
	s.mu.Lock()
	if s.waiting != nil && s.waiting.Username == username {
		s.waiting = nil
		waitingCleared = true
		if s.waitingTimer != nil {
			s.waitingTimer.Stop()
			s.waitingTimer = nil
		}
	}
	for id, g := range s.games {
		for _, p := range g.Players {
			if p != nil && p.Username == username && g.Status == game.StatusWaiting {
				delete(s.games, id)
				break
			}
		}
	}
	if activeGame := s.findGameByPlayer(username); activeGame != nil {
		delete(s.rematchRequests, activeGame.ID)
		delete(s.rematchRequesters, activeGame.ID)
		if timer, ok := s.rematchTimers[activeGame.ID]; ok {
			timer.Stop()
			delete(s.rematchTimers, activeGame.ID)
		}
	}
	if existingConn, ok := s.connections[username]; ok {
		conn = existingConn
		s.connections[username] = nil
	}
	s.mu.Unlock()

	if waitingCleared {
		s.announceStatus(username, "Left the queue.")
	}
	s.handleDisconnect(username)
	if conn != nil {
		_ = conn.WriteJSON(map[string]string{"type": "status", "message": "Logged out."})
		_ = conn.Close()
	}
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
