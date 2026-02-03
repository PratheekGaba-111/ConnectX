package server

import (
	"fmt"
	"log"
	"time"

	"connectx/internal/game"
)

func (s *Server) handleMove(g *game.Game, player *game.Player, col int) {
	if g.Status != game.StatusActive {
		return
	}
	if g.CurrentPlayer().Username != player.Username {
		s.sendMessage(player.Username, "error", "Not your turn")
		return
	}
	_, err := g.DropDisc(col)
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
		s.announceStatus(player.Username, "Looking for a new opponent...")
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
