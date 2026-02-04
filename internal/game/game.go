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
	bestCol := -1
	bestScore := -1 << 30
	for col := 0; col < Columns; col++ {
		if g.Board[0][col] != 0 {
			continue
		}
		row := g.findDropRow(col)
		if row == -1 {
			continue
		}
		g.Board[row][col] = botID
		score := g.scorePosition(botID, opponentID)
		if g.hasImmediateWin(opponentID) {
			score -= 500
		}
		g.Board[row][col] = 0
		if score > bestScore {
			bestScore = score
			bestCol = col
		}
	}
	if bestCol != -1 {
		return bestCol
	}
	return rand.Intn(Columns)
}

func (g *Game) findDropRow(col int) int {
	for r := Rows - 1; r >= 0; r-- {
		if g.Board[r][col] == 0 {
			return r
		}
	}
	return -1
}

func (g *Game) hasImmediateWin(playerID int) bool {
	for col := 0; col < Columns; col++ {
		if g.Board[0][col] != 0 {
			continue
		}
		row := g.findDropRow(col)
		if row == -1 {
			continue
		}
		g.Board[row][col] = playerID
		win := g.checkWin(playerID, row, col)
		g.Board[row][col] = 0
		if win {
			return true
		}
	}
	return false
}

func (g *Game) scorePosition(botID, opponentID int) int {
	score := 0
	center := Columns / 2
	for r := 0; r < Rows; r++ {
		if g.Board[r][center] == botID {
			score += 3
		}
	}
	score += g.scoreWindows(botID, opponentID, 0, 1)
	score += g.scoreWindows(botID, opponentID, 1, 0)
	score += g.scoreWindows(botID, opponentID, 1, 1)
	score += g.scoreWindows(botID, opponentID, 1, -1)
	return score
}

func (g *Game) scoreWindows(botID, opponentID, dr, dc int) int {
	score := 0
	for r := 0; r < Rows; r++ {
		for c := 0; c < Columns; c++ {
			endR := r + dr*(WinLen-1)
			endC := c + dc*(WinLen-1)
			if endR < 0 || endR >= Rows || endC < 0 || endC >= Columns {
				continue
			}
			botCount := 0
			opponentCount := 0
			emptyCount := 0
			for i := 0; i < WinLen; i++ {
				cell := g.Board[r+i*dr][c+i*dc]
				switch cell {
				case botID:
					botCount++
				case opponentID:
					opponentCount++
				default:
					emptyCount++
				}
			}
			score += evaluateWindow(botCount, opponentCount, emptyCount)
		}
	}
	return score
}

func evaluateWindow(botCount, opponentCount, emptyCount int) int {
	if opponentCount == 0 {
		switch botCount {
		case 2:
			return 4
		case 3:
			return 12
		case 4:
			return 100
		}
	}
	if botCount == 0 && opponentCount == 3 && emptyCount == 1 {
		return -8
	}
	return 0
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
