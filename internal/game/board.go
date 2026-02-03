package game

import (
	"errors"
	"math/rand"
	"time"
)

const (
	Columns = 7
	Rows    = 6
	WinLen  = 4
)

type Move struct {
	PlayerID int
	Row      int
	Col      int
}

func (g *Game) DropDisc(col int) (*Move, error) {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	if g.Status != StatusActive {
		return nil, errors.New("game not active")
	}
	if col < 0 || col >= Columns {
		return nil, errors.New("invalid column")
	}
	row := -1
	for r := Rows - 1; r >= 0; r-- {
		if g.Board[r][col] == 0 {
			row = r
			break
		}
	}
	if row == -1 {
		return nil, errors.New("column full")
	}
	playerID := g.CurrentPlayer().ID
	g.Board[row][col] = playerID
	move := &Move{PlayerID: playerID, Row: row, Col: col}
	g.LastMove = move
	if g.checkWin(playerID, row, col) {
		g.Status = StatusFinished
		g.WinnerID = playerID
		g.EndedAt = time.Now()
		return move, nil
	}
	if g.isDraw() {
		g.Status = StatusFinished
		g.WinnerID = 0
		g.EndedAt = time.Now()
		return move, nil
	}
	g.Turn = (g.Turn + 1) % 2
	g.TurnStartedAt = time.Now()
	return move, nil
}

func (g *Game) RandomValidColumn() int {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	available := []int{}
	for col := 0; col < Columns; col++ {
		if g.Board[0][col] == 0 {
			available = append(available, col)
		}
	}
	if len(available) == 0 {
		return -1
	}
	return available[rand.Intn(len(available))]
}

func (g *Game) checkWin(playerID, row, col int) bool {
	directions := [][2]int{{0, 1}, {1, 0}, {1, 1}, {1, -1}}
	for _, dir := range directions {
		count := 1
		count += g.countDirection(playerID, row, col, dir[0], dir[1])
		count += g.countDirection(playerID, row, col, -dir[0], -dir[1])
		if count >= WinLen {
			return true
		}
	}
	return false
}

func (g *Game) countDirection(playerID, row, col, dr, dc int) int {
	count := 0
	r := row + dr
	c := col + dc
	for r >= 0 && r < Rows && c >= 0 && c < Columns {
		if g.Board[r][c] != playerID {
			break
		}
		count++
		r += dr
		c += dc
	}
	return count
}

func (g *Game) isDraw() bool {
	for c := 0; c < Columns; c++ {
		if g.Board[0][c] == 0 {
			return false
		}
	}
	return true
}
