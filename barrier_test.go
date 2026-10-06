package cyclicbarrier

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"
)

// waitTimeout is only a deadlock guard, never used for synchronization.
const waitTimeout = 10 * time.Second

type waitResult struct {
	gen uint64
	err error
}

// waitAsync runs Wait in a goroutine and delivers its result.
func waitAsync(b *Barrier, ctx context.Context) <-chan waitResult {
	ch := make(chan waitResult, 1)
	go func() {
		gen, err := b.Wait(ctx)
		ch <- waitResult{gen: gen, err: err}
	}()
	return ch
}

// recv waits for a Wait result, failing the test on deadlock.
func recv(t *testing.T, ch <-chan waitResult) waitResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(waitTimeout):
		t.Fatal("timed out waiting for Wait to return; possible deadlock")
		return waitResult{}
	}
}

// waitForArrived spins with Gosched (no sleeps) until Arrived == want.
func waitForArrived(t *testing.T, b *Barrier, want int) {
	t.Helper()
	timeout := time.After(waitTimeout)
	for b.Snapshot().Arrived != want {
		select {
		case <-timeout:
			t.Fatalf("timed out waiting for Arrived=%d", want)
		default:
			runtime.Gosched()
		}
	}
}

func mustNew(t *testing.T, parties int) *Barrier {
	t.Helper()
	b, err := New(parties)
	if err != nil {
		t.Fatalf("New(%d): %v", parties, err)
	}
	return b
}

func TestNewRejectsNonPositiveParties(t *testing.T) {
	for _, parties := range []int{0, -1, -100} {
		if b, err := New(parties); err == nil || b != nil {
			t.Errorf("New(%d) = (%v, %v), want (nil, error)", parties, b, err)
		}
	}
	b, err := New(1)
	if err != nil || b == nil {
		t.Fatalf("New(1) = (%v, %v), want a valid barrier", b, err)
	}
	if got := b.Snapshot().Parties; got != 1 {
		t.Fatalf("Parties = %d, want 1", got)
	}
}

func TestWaitReleasesGenerationTogether(t *testing.T) {
	const parties = 3
	b := mustNew(t, parties)

	results := make([]<-chan waitResult, 0, parties)
	for i := 0; i < parties; i++ {
		results = append(results, waitAsync(b, context.Background()))
	}
	for i, ch := range results {
		r := recv(t, ch)
		if r.err != nil {
			t.Fatalf("waiter %d: unexpected error: %v", i, r.err)
		}
		if r.gen != 0 {
			t.Errorf("waiter %d: gen = %d, want 0", i, r.gen)
		}
	}
	if s := b.Snapshot(); s.Generation != 1 || s.Arrived != 0 {
		t.Errorf("Snapshot = %+v, want Generation=1 Arrived=0", s)
	}
}

func TestMultipleGenerationsStayIsolated(t *testing.T) {
	const parties = 2
	const rounds = 3
	b := mustNew(t, parties)

	gens := make([][]uint64, parties)
	var wg sync.WaitGroup
	for i := 0; i < parties; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				gen, err := b.Wait(context.Background())
				if err != nil {
					t.Errorf("waiter %d round %d: %v", id, r, err)
					return
				}
				gens[id] = append(gens[id], gen)
			}
		}(i)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(waitTimeout):
		t.Fatal("timed out; possible deadlock")
	}

	for id := range gens {
		if want := []uint64{0, 1, 2}; !slices.Equal(gens[id], want) {
			t.Errorf("waiter %d gens = %v, want %v", id, gens[id], want)
		}
	}
	if s := b.Snapshot(); s.Generation != rounds || s.Arrived != 0 {
		t.Errorf("Snapshot = %+v, want Generation=%d Arrived=0", s, rounds)
	}
}

func TestWaitCancelWithdrawsOnlyItself(t *testing.T) {
	b := mustNew(t, 2)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := b.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait with canceled ctx = %v, want context.Canceled", err)
	}
	if s := b.Snapshot(); s.Arrived != 0 || s.Generation != 0 {
		t.Fatalf("Snapshot after cancel = %+v, want Generation=0 Arrived=0", s)
	}

	// The barrier itself is unaffected: two fresh waiters trip generation 0.
	a := waitAsync(b, context.Background())
	c := waitAsync(b, context.Background())
	for i, ch := range []<-chan waitResult{a, c} {
		if r := recv(t, ch); r.err != nil || r.gen != 0 {
			t.Errorf("waiter %d = (%d, %v), want (0, nil)", i, r.gen, r.err)
		}
	}
}

func TestCancelThenRefillReleasesGeneration(t *testing.T) {
	b := mustNew(t, 3)

	parked := waitAsync(b, context.Background())
	waitForArrived(t, b, 1)

	ctx, cancel := context.WithCancel(context.Background())
	canceled := waitAsync(b, ctx)
	waitForArrived(t, b, 2)
	cancel()
	if r := recv(t, canceled); !errors.Is(r.err, context.Canceled) {
		t.Fatalf("canceled waiter = (%d, %v), want context.Canceled", r.gen, r.err)
	}
	waitForArrived(t, b, 1)

	// Refill the freed slot: two more arrivals trip generation 0.
	filler1 := waitAsync(b, context.Background())
	filler2 := waitAsync(b, context.Background())
	for name, ch := range map[string]<-chan waitResult{
		"parked": parked, "filler1": filler1, "filler2": filler2,
	} {
		if r := recv(t, ch); r.err != nil || r.gen != 0 {
			t.Errorf("%s = (%d, %v), want (0, nil)", name, r.gen, r.err)
		}
	}
	if s := b.Snapshot(); s.Generation != 1 || s.Arrived != 0 {
		t.Errorf("Snapshot = %+v, want Generation=1 Arrived=0", s)
	}
}

func TestCancelRacingWithLastArrivalStaysConsistent(t *testing.T) {
	for i := 0; i < 8; i++ {
		b := mustNew(t, 2)
		ctx, cancel := context.WithCancel(context.Background())
		racer := waitAsync(b, ctx)
		waitForArrived(t, b, 1)

		start := make(chan struct{})
		last := make(chan waitResult, 1)
		go func() {
			<-start
			cancel()
		}()
		go func() {
			<-start
			gen, err := b.Wait(context.Background())
			last <- waitResult{gen: gen, err: err}
		}()
		close(start)

		r := recv(t, racer)
		if r.err != nil {
			// Cancellation won: the last arrival is parked alone; refill.
			if !errors.Is(r.err, context.Canceled) {
				t.Fatalf("iter %d: racer err = %v, want context.Canceled", i, r.err)
			}
			waitForArrived(t, b, 1)
			filler := waitAsync(b, context.Background())
			if fr := recv(t, filler); fr.err != nil || fr.gen != 0 {
				t.Fatalf("iter %d: filler = (%d, %v), want (0, nil)", i, fr.gen, fr.err)
			}
			if lr := recv(t, last); lr.err != nil || lr.gen != 0 {
				t.Fatalf("iter %d: last = (%d, %v), want (0, nil)", i, lr.gen, lr.err)
			}
		} else {
			// The trip won: both participants succeed with generation 0.
			if r.gen != 0 {
				t.Fatalf("iter %d: racer gen = %d, want 0", i, r.gen)
			}
			if lr := recv(t, last); lr.err != nil || lr.gen != 0 {
				t.Fatalf("iter %d: last = (%d, %v), want (0, nil)", i, lr.gen, lr.err)
			}
		}
		if s := b.Snapshot(); s.Generation != 1 || s.Arrived != 0 {
			t.Fatalf("iter %d: Snapshot = %+v, want Generation=1 Arrived=0", i, s)
		}
	}
}

func TestSnapshotReturnsIndependentCopy(t *testing.T) {
	b := mustNew(t, 3)

	if s := b.Snapshot(); s != (Snapshot{Generation: 0, Arrived: 0, Parties: 3, Closed: false}) {
		t.Fatalf("initial Snapshot = %+v", s)
	}

	a := waitAsync(b, context.Background())
	waitForArrived(t, b, 1)
	s := b.Snapshot()
	if s.Arrived != 1 || s.Generation != 0 || s.Parties != 3 || s.Closed {
		t.Fatalf("Snapshot with one arrival = %+v", s)
	}

	// Mutating the returned snapshot must not affect the barrier.
	s.Arrived = 99
	s.Closed = true
	if again := b.Snapshot(); again.Arrived != 1 || again.Closed {
		t.Fatalf("Snapshot after mutation = %+v, want Arrived=1 Closed=false", again)
	}

	c := waitAsync(b, context.Background())
	d := waitAsync(b, context.Background())
	for _, ch := range []<-chan waitResult{a, c, d} {
		recv(t, ch)
	}
	if s := b.Snapshot(); s.Generation != 1 || s.Arrived != 0 {
		t.Fatalf("Snapshot after trip = %+v, want Generation=1 Arrived=0", s)
	}
}

func TestCloseWakesWaitersAndRejectsNewWaits(t *testing.T) {
	b := mustNew(t, 2)

	parked := waitAsync(b, context.Background())
	waitForArrived(t, b, 1)

	b.Close()
	if r := recv(t, parked); !errors.Is(r.err, ErrClosed) {
		t.Fatalf("parked waiter = (%d, %v), want ErrClosed", r.gen, r.err)
	}
	if _, err := b.Wait(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Wait after Close = %v, want ErrClosed", err)
	}
	if s := b.Snapshot(); !s.Closed || s.Arrived != 0 {
		t.Fatalf("Snapshot after Close = %+v, want Closed=true Arrived=0", s)
	}

	// Close is idempotent.
	b.Close()
	if _, err := b.Wait(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Wait after second Close = %v, want ErrClosed", err)
	}
}

func TestConcurrentWaitersAcrossGenerations(t *testing.T) {
	const parties = 8
	const rounds = 3
	b := mustNew(t, parties)

	gens := make([][]uint64, parties)
	var wg sync.WaitGroup
	for i := 0; i < parties; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				gen, err := b.Wait(context.Background())
				if err != nil {
					t.Errorf("waiter %d round %d: %v", id, r, err)
					return
				}
				gens[id] = append(gens[id], gen)
			}
		}(i)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(waitTimeout):
		t.Fatal("timed out; possible deadlock")
	}

	for id := range gens {
		if want := []uint64{0, 1, 2}; !slices.Equal(gens[id], want) {
			t.Errorf("waiter %d gens = %v, want %v", id, gens[id], want)
		}
	}
	if s := b.Snapshot(); s.Generation != rounds || s.Arrived != 0 {
		t.Errorf("Snapshot = %+v, want Generation=%d Arrived=0", s, rounds)
	}
}
