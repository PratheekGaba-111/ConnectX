package game

type Status string

const (
	StatusWaiting  Status = "waiting"
	StatusActive   Status = "active"
	StatusFinished Status = "finished"
)
