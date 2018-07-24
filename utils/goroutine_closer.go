package utils

import (
	"sync"
)

type Closer struct {
	closed       chan int
	exit_wg      sync.WaitGroup
	exit_wg_chan chan int
}

func NewCloser() *Closer {
	return &Closer{
		closed: make(chan int),
	}
}

func (lc *Closer) AddOne() {
	lc.exit_wg.Add(1)
}

func (lc *Closer) Signal() {
	close(lc.closed)
}

func (lc *Closer) HasBeenClosed() <-chan int {
	return lc.closed
}

func (lc *Closer) Done() {
	lc.exit_wg.Done()
}

func (lc *Closer) Wait() {
	lc.exit_wg.Wait()
}

func (lc *Closer) SignalAndWait() {
	lc.Signal()
	lc.Wait()
}

func (lc *Closer) SignalAndAsyncWait() {
	lc.Signal()
	lc.exit_wg_chan = make(chan int)
	go func() {
		lc.Wait()
		close(lc.exit_wg_chan)
	}()
}

func (lc *Closer) CloseCompleted() <-chan int {
	return lc.exit_wg_chan
}
