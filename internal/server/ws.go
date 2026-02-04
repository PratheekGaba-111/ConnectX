package server

import (
	"log"
	"net/http"
	"strings"

	"connectx/internal/game"

	"github.com/gorilla/websocket"
)

type wsMessage struct {
	Type     string `json:"type"`
	Column   int    `json:"column,omitempty"`
	Username string `json:"username,omitempty"`
	GameID   string `json:"gameId,omitempty"`
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
	if isRejoin && gameInstance != nil {
		log.Printf("%s rejoined game %s", username, gameInstance.ID)
	}

	for {
		var msg wsMessage
		if err := conn.ReadJSON(&msg); err != nil {
			s.handleDisconnect(player.Username)
			return
		}
		var activeGame *game.Game
		s.mu.Lock()
		for _, g := range s.games {
			for _, p := range g.Players {
				if p != nil && p.Username == player.Username {
					activeGame = g
					break
				}
			}
			if activeGame != nil {
				break
			}
		}
		s.mu.Unlock()
		switch msg.Type {
		case "move":
			if activeGame != nil {
				s.handleMove(activeGame, player, msg.Column)
			}
		case "reset":
			if activeGame != nil {
				s.handleReset(activeGame, player)
			}
		case "new_game":
			s.handleNewGame(player)
		case "rematch_accept":
			if activeGame != nil {
				s.handleRematchAccept(activeGame, player)
			}
		case "rematch_reject":
			if activeGame != nil {
				s.handleRematchReject(activeGame, player)
			}
		case "logout":
			s.handleLogout(player)
			return
		}
	}
}
