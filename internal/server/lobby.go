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
	var notifyMessage string
	var oldGame *game.Game
	var shouldFinalize bool
	var concludedGame *game.Game
	var concludedBotGame bool
	s.mu.Lock()
	s.removeFromQueueLocked(player.Username)
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
			if oldGame.Status == game.StatusActive {
				notifyMessage = "Opponent forfeited the match, you won."
			} else if oldGame.Status == game.StatusFinished {
				notifyMessage = "Opponent Left"
			}
		}
		if oldGame.Status == game.StatusActive {
			oldGame.Status = game.StatusFinished
			oldGame.EndedAt = time.Now()
			if opponent != nil {
				oldGame.WinnerID = opponent.ID
			}
			shouldFinalize = true
			concludedGame = oldGame
			concludedBotGame = (oldGame.Players[0] != nil && oldGame.Players[0].IsBot) ||
				(oldGame.Players[1] != nil && oldGame.Players[1].IsBot)
		}
		delete(s.games, oldGame.ID)
	}

	var gameInstance *game.Game
	opponent := s.popWaitingLocked()
	if opponent == nil {
		player.ID = 1
		gameInstance = s.addToQueueLocked(player)
	} else {
		player.ID = 2
		opponent.ID = 1
		gameInstance = game.NewGame(randomID(), opponent, player)
		s.games[gameInstance.ID] = gameInstance
	}
	s.mu.Unlock()

	if shouldFinalize {
		s.finalizeGame(oldGame)
	}
	if concludedGame != nil {
		s.sendState(concludedGame, "", concludedBotGame)
	}
	if notifyOpponent != "" && notifyMessage != "" {
		s.sendMessage(notifyOpponent, "status", notifyMessage)
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
	waitingCleared = s.removeFromQueueLocked(username)
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
	if len(s.waitingQueue) != 1 || s.waitingQueue[0] != username {
		s.mu.Unlock()
		return
	}
	s.waitingQueue = s.waitingQueue[:0]
	if timer, ok := s.waitingTimers[username]; ok {
		timer.Stop()
		delete(s.waitingTimers, username)
	}
	player, ok := s.players[username]
	if !ok || player == nil {
		s.deleteWaitingGameLocked(username)
		s.mu.Unlock()
		return
	}
	bot := &game.Player{Username: "Bot", ID: 2, IsBot: true, Online: true}
	gameInstance := s.findGameByPlayer(player.Username)
	if gameInstance == nil || gameInstance.Status != game.StatusWaiting {
		s.deleteWaitingGameLocked(username)
		gameInstance = game.NewGame(randomID(), player, bot)
		s.games[gameInstance.ID] = gameInstance
	} else {
		gameInstance.Players[1] = bot
		gameInstance.Status = game.StatusActive
		gameInstance.StartedAt = time.Now()
		gameInstance.TurnStartedAt = time.Now()
	}
	s.mu.Unlock()

	go s.emitEvent("game_started", gameInstance, "bot")
	s.sendState(gameInstance, "Bot opponent joined", true)
	s.startTurnTimer(gameInstance)
}

func (s *Server) addToQueueLocked(player *game.Player) *game.Game {
	s.deleteWaitingGameLocked(player.Username)
	s.waitingQueue = append(s.waitingQueue, player.Username)
	if timer, ok := s.waitingTimers[player.Username]; ok {
		timer.Stop()
	}
	s.waitingTimers[player.Username] = time.AfterFunc(10*time.Second, func() {
		s.startBotGame(player.Username)
	})
	waitingGame := game.NewWaitingGame(randomID(), player)
	s.games[waitingGame.ID] = waitingGame
	return waitingGame
}

func (s *Server) popWaitingLocked() *game.Player {
	for len(s.waitingQueue) > 0 {
		username := s.waitingQueue[0]
		s.waitingQueue = s.waitingQueue[1:]
		if timer, ok := s.waitingTimers[username]; ok {
			timer.Stop()
			delete(s.waitingTimers, username)
		}
		player, ok := s.players[username]
		s.deleteWaitingGameLocked(username)
		if ok && player != nil && player.Online {
			return player
		}
	}
	return nil
}

func (s *Server) removeFromQueueLocked(username string) bool {
	removed := false
	for i, queued := range s.waitingQueue {
		if queued == username {
			s.waitingQueue = append(s.waitingQueue[:i], s.waitingQueue[i+1:]...)
			removed = true
			break
		}
	}
	if timer, ok := s.waitingTimers[username]; ok {
		timer.Stop()
		delete(s.waitingTimers, username)
	}
	s.deleteWaitingGameLocked(username)
	return removed
}

func (s *Server) deleteWaitingGameLocked(username string) {
	for id, g := range s.games {
		if g.Status != game.StatusWaiting {
			continue
		}
		for _, p := range g.Players {
			if p != nil && p.Username == username {
				delete(s.games, id)
				return
			}
		}
	}
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
