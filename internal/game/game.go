package game

import (
	"errors"
	"math/rand"
	"sync"
	"time"
)

const (
	Columns = 7
	Rows    = 6
	WinLen  = 4
)

type Status string

const (
	StatusWaiting  Status = "waiting"
	StatusActive   Status = "active"
	StatusFinished Status = "finished"
)

type Player struct {
	Username string
	ID       int
	IsBot    bool
	ConnID   string
	Online   bool
}

type Move struct {
	PlayerID int
	Row      int
	Col      int
}

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
