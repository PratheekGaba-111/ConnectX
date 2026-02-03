package game

import (
	"math/rand"
	"sync"
	"time"
)

type Game struct {
	ID            string
	Board         [][]int
	Players       [2]*Player
	Turn          int
	Status        Status
	WinnerID      int
	StartedAt     time.Time
	TurnStartedAt time.Time
	EndedAt       time.Time
	LastMove      *Move
	mutex         sync.Mutex
}

func NewGame(id string, p1 *Player, p2 *Player) *Game {
	board := make([][]int, Rows)
	for r := range board {
		board[r] = make([]int, Columns)
	}
	return &Game{
		ID:            id,
		Board:         board,
		Players:       [2]*Player{p1, p2},
		Turn:          0,
		Status:        StatusActive,
		StartedAt:     time.Now(),
		TurnStartedAt: time.Now(),
	}
}

func NewWaitingGame(id string, p1 *Player) *Game {
	board := make([][]int, Rows)
	for r := range board {
		board[r] = make([]int, Columns)
	}
	return &Game{
		ID:      id,
		Board:   board,
		Players: [2]*Player{p1, nil},
		Turn:    0,
		Status:  StatusWaiting,
	}
}

func (g *Game) CurrentPlayer() *Player {
	return g.Players[g.Turn]
}

func (g *Game) CopyBoard() [][]int {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	board := make([][]int, Rows)
	for r := range board {
		board[r] = make([]int, Columns)
		copy(board[r], g.Board[r])
	}
	return board
}

func (g *Game) FindStrategicMove(botID, opponentID int) int {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	if winningCol := g.findWinningColumn(botID); winningCol != -1 {
		return winningCol
	}
	if blockCol := g.findWinningColumn(opponentID); blockCol != -1 {
		return blockCol
	}
	preferred := []int{3, 2, 4, 1, 5, 0, 6}
	for _, col := range preferred {
		if g.Board[0][col] == 0 {
			return col
		}
	}
	return rand.Intn(Columns)
}

func (g *Game) findWinningColumn(playerID int) int {
	for col := 0; col < Columns; col++ {
		row := -1
		for r := Rows - 1; r >= 0; r-- {
			if g.Board[r][col] == 0 {
				row = r
				break
			}
		}
		if row == -1 {
			continue
		}
		g.Board[row][col] = playerID
		win := g.checkWin(playerID, row, col)
		g.Board[row][col] = 0
		if win {
			return col
		}
	}
	return -1
}
