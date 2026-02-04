package server

import (
	"fmt"
	"log"
	"time"

	"connectx/internal/game"

	"github.com/gorilla/websocket"
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
	if err := s.recordWin(winner); err != nil {
		log.Printf("record win error: %v", err)
	}
	go s.emitEvent("game_completed", g, winner)
	s.cleanupGame(g)
}

func (s *Server) handleReset(g *game.Game, player *game.Player) {
	s.mu.Lock()
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
	}
	s.mu.Unlock()
	s.cleanupGame(g)
}

func (s *Server) cleanupGame(g *game.Game) {
	if g == nil {
		return
	}
	var conns []*websocket.Conn
	var usernames []string
	s.mu.Lock()
	delete(s.games, g.ID)
	for _, p := range g.Players {
		if p == nil || p.IsBot {
			continue
		}
		usernames = append(usernames, p.Username)
		if timer, ok := s.forfeitTimers[p.Username]; ok {
			timer.Stop()
			delete(s.forfeitTimers, p.Username)
		}
		if conn, ok := s.connections[p.Username]; ok && conn != nil {
			conns = append(conns, conn)
			s.connections[p.Username] = nil
		}
		delete(s.disconnectAt, p.Username)
	}
	s.mu.Unlock()
	if err := s.deleteGameRecord(g.ID); err != nil {
		log.Printf("delete game error: %v", err)
	}
	for i, conn := range conns {
		username := ""
		if i < len(usernames) {
			username = usernames[i]
		}
		if username != "" {
			_ = conn.WriteJSON(map[string]string{"type": "status", "message": "Game completed. Please join again to start a new session."})
		}
		_ = conn.Close()
	}
}
