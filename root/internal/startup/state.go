package startup

import "sync/atomic"

type State struct {
	ready atomic.Bool
}

func NewState() *State {
	return &State{}
}

func (s *State) MarkReady() {
	s.ready.Store(true)
}

func (s *State) IsReady() bool {
	return s.ready.Load()
}
