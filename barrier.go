// Package cyclicbarrier provides a concurrency-safe cyclic barrier.
//
// A Barrier is created for a fixed number of parties. Each call to Wait
// joins the current generation; once all parties have arrived, every
// participant of that generation is released with the generation number
// and the barrier automatically advances to the next generation.
package cyclicbarrier

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrClosed is returned by Wait when the barrier has been closed.
var ErrClosed = errors.New("cyclicbarrier: barrier is closed")

// Snapshot is an independent copy of the barrier state at a point in time.
type Snapshot struct {
	Generation uint64
	Arrived    int
	Parties    int
	Closed     bool
}

// Barrier is a cyclic synchronization barrier. The zero value is not
// usable; create one with NewBarrier.
type Barrier struct {
	mu         sync.Mutex
	parties    int
	arrived    int
	generation uint64
	closed     bool
	release    chan struct{} // closed when the generation trips or the barrier closes
}

// NewBarrier returns a Barrier that releases every parties arrivals.
// It returns an error if parties <= 0.
func NewBarrier(parties int) (*Barrier, error) {
	if parties <= 0 {
		return nil, fmt.Errorf("cyclicbarrier: parties must be > 0, got %d", parties)
	}
	return &Barrier{parties: parties, release: make(chan struct{})}, nil
}

// Wait joins the current generation and blocks until all parties have
// arrived, then returns the number of the completed generation
// (generations are numbered from 0).
//
// If ctx is cancelled while waiting, only this caller is withdrawn and
// Wait returns ctx.Err(); the barrier itself is not tripped. If the
// cancellation races with the final arrival that trips the generation,
// Wait may return success; the arrival count stays consistent either way.
//
// After Close, Wait returns ErrClosed immediately.
func (b *Barrier) Wait(ctx context.Context) (uint64, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return 0, ErrClosed
	}
	gen := b.generation
	b.arrived++
	if b.arrived == b.parties {
		b.tripLocked()
		b.mu.Unlock()
		return gen, nil
	}
	ch := b.release
	b.mu.Unlock()

	select {
	case <-ch:
		b.mu.Lock()
		defer b.mu.Unlock()
		if b.generation != gen {
			return gen, nil
		}
		b.arrived--
		return 0, ErrClosed
	case <-ctx.Done():
		b.mu.Lock()
		defer b.mu.Unlock()
		if b.generation != gen {
			// The generation tripped before the withdrawal took
			// effect, so this caller counts as released.
			return gen, nil
		}
		b.arrived--
		return 0, ctx.Err()
	}
}

// Snapshot returns an independent copy of the current barrier state.
func (b *Barrier) Snapshot() Snapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	return Snapshot{
		Generation: b.generation,
		Arrived:    b.arrived,
		Parties:    b.parties,
		Closed:     b.closed,
	}
}

// Close wakes all current waiters with ErrClosed and rejects any further
// Wait calls. It is idempotent.
func (b *Barrier) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	close(b.release)
}

// tripLocked releases the current generation and starts the next one.
// b.mu must be held.
func (b *Barrier) tripLocked() {
	b.generation++
	b.arrived = 0
	close(b.release)
	b.release = make(chan struct{})
}
