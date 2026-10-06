// Package cyclicbarrier implements a concurrency-safe cyclic barrier.
//
// A Barrier is created for a fixed number of parties. Every call to Wait
// joins the current generation; once all parties have arrived, every
// participant of that generation is released with the generation number
// and the barrier automatically advances to the next generation.
// Generations are strictly isolated from each other.
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
	// Generation is the current generation number (the first generation is 0).
	Generation uint64
	// Arrived is the number of parties currently waiting in this generation.
	Arrived int
	// Parties is the fixed number of parties required to trip the barrier.
	Parties int
	// Closed reports whether Close has been called.
	Closed bool
}

// Barrier is a cyclic synchronization barrier. Create one with New.
type Barrier struct {
	mu         sync.Mutex
	parties    int
	arrived    int
	generation uint64
	closed     bool
	release    chan struct{} // closed to release every waiter of the current generation
}

// New returns a Barrier that trips once parties goroutines have arrived.
// It returns an error if parties <= 0.
func New(parties int) (*Barrier, error) {
	if parties <= 0 {
		return nil, fmt.Errorf("cyclicbarrier: parties must be positive, got %d", parties)
	}
	return &Barrier{parties: parties, release: make(chan struct{})}, nil
}

// Wait blocks until the current generation trips, the barrier is closed,
// or ctx is done.
//
// On success Wait returns the number of the completed generation. If ctx is
// done before the generation trips, Wait withdraws only itself and returns
// ctx.Err(); the remaining parties are unaffected. If the last arrival races
// with a cancellation, the generation trip wins and Wait reports success, so
// the arrival count stays consistent. After Close, Wait returns ErrClosed.
func (b *Barrier) Wait(ctx context.Context) (uint64, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return 0, ErrClosed
	}
	gen := b.generation
	release := b.release
	b.arrived++
	if b.arrived == b.parties {
		b.tripLocked()
		b.mu.Unlock()
		return gen, nil
	}
	b.mu.Unlock()

	select {
	case <-release:
		b.mu.Lock()
		defer b.mu.Unlock()
		if b.generation != gen {
			return gen, nil
		}
		// The channel was closed by Close, not by a generation trip.
		return 0, ErrClosed
	case <-ctx.Done():
		b.mu.Lock()
		defer b.mu.Unlock()
		if b.generation != gen {
			// The generation tripped concurrently with the cancellation;
			// this arrival was already counted, so report success.
			return gen, nil
		}
		if b.closed {
			return 0, ErrClosed
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

// Close wakes every current waiter with ErrClosed and rejects future Wait
// calls. It is idempotent and safe to call concurrently with Wait.
func (b *Barrier) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	b.arrived = 0
	close(b.release)
	b.release = make(chan struct{})
}

// tripLocked releases the current generation and starts the next one.
// b.mu must be held.
func (b *Barrier) tripLocked() {
	b.generation++
	b.arrived = 0
	close(b.release)
	b.release = make(chan struct{})
}
