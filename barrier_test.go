package cyclicbarrier

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
)

type waitResult struct {
	gen uint64
	err error
}

func mustNew(t *testing.T, parties int) *Barrier {
	t.Helper()
	b, err := NewBarrier(parties)
	if err != nil {
		t.Fatalf("NewBarrier(%d): %v", parties, err)
	}
	return b
}

func waitAsync(b *Barrier, ctx context.Context) <-chan waitResult {
	ch := make(chan waitResult, 1)
	go func() {
		gen, err := b.Wait(ctx)
		ch <- waitResult{gen, err}
	}()
	return ch
}

func recvResult(t *testing.T, ch <-chan waitResult) waitResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Wait to return")
		return waitResult{}
	}
}

func waitForCondition(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 1_000_000; i++ {
		if cond() {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("condition not satisfied: %s", what)
}

func waitArrived(t *testing.T, b *Barrier, n int) {
	t.Helper()
	waitForCondition(t, "arrived count", func() bool {
		return b.Snapshot().Arrived == n
	})
}

func TestNewBarrierInvalidParties(t *testing.T) {
	for _, parties := range []int{0, -1, -100} {
		if b, err := NewBarrier(parties); err == nil || b != nil {
			t.Errorf("NewBarrier(%d) = (%v, %v), want (nil, error)", parties, b, err)
		}
	}
}

func TestReleaseWhenAllArrive(t *testing.T) {
	b := mustNew(t, 3)
	chs := make([]<-chan waitResult, 3)
	for i := range chs {
		chs[i] = waitAsync(b, context.Background())
	}
	for _, ch := range chs {
		r := recvResult(t, ch)
		if r.err != nil || r.gen != 0 {
			t.Errorf("Wait = (%d, %v), want (0, nil)", r.gen, r.err)
		}
	}
	if s := b.Snapshot(); s.Generation != 1 || s.Arrived != 0 {
		t.Errorf("Snapshot = %+v, want Generation=1 Arrived=0", s)
	}
}

func TestWaitBlocksUntilPartiesArrive(t *testing.T) {
	b := mustNew(t, 2)
	ch1 := waitAsync(b, context.Background())
	waitArrived(t, b, 1)
	select {
	case r := <-ch1:
		t.Fatalf("Wait returned before all parties arrived: %+v", r)
	default:
	}
	ch2 := waitAsync(b, context.Background())
	for _, ch := range []<-chan waitResult{ch1, ch2} {
		r := recvResult(t, ch)
		if r.err != nil || r.gen != 0 {
			t.Errorf("Wait = (%d, %v), want (0, nil)", r.gen, r.err)
		}
	}
}

func TestMultipleGenerations(t *testing.T) {
	b := mustNew(t, 3)
	const rounds = 3
	results := make([][]waitResult, 3)
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				gen, err := b.Wait(context.Background())
				results[idx] = append(results[idx], waitResult{gen, err})
			}
		}(i)
	}
	wg.Wait()
	for i, rs := range results {
		if len(rs) != rounds {
			t.Fatalf("goroutine %d completed %d rounds, want %d", i, len(rs), rounds)
		}
		for r, res := range rs {
			if res.err != nil || res.gen != uint64(r) {
				t.Errorf("goroutine %d round %d: Wait = (%d, %v), want (%d, nil)",
					i, r, res.gen, res.err, r)
			}
		}
	}
	if s := b.Snapshot(); s.Generation != rounds || s.Arrived != 0 {
		t.Errorf("Snapshot = %+v, want Generation=%d Arrived=0", s, rounds)
	}
}

func TestContextCancelWithdraws(t *testing.T) {
	b := mustNew(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	ch := waitAsync(b, ctx)
	waitArrived(t, b, 1)
	cancel()
	if r := recvResult(t, ch); !errors.Is(r.err, context.Canceled) {
		t.Fatalf("cancelled Wait err = %v, want context.Canceled", r.err)
	}
	waitArrived(t, b, 0)

	// The barrier is untripped and still usable for generation 0.
	ch1 := waitAsync(b, context.Background())
	ch2 := waitAsync(b, context.Background())
	for _, c := range []<-chan waitResult{ch1, ch2} {
		r := recvResult(t, c)
		if r.err != nil || r.gen != 0 {
			t.Errorf("Wait = (%d, %v), want (0, nil)", r.gen, r.err)
		}
	}
	if s := b.Snapshot(); s.Generation != 1 {
		t.Errorf("Generation = %d, want 1", s.Generation)
	}
}

func TestCancelThenFill(t *testing.T) {
	b := mustNew(t, 3)
	ch1 := waitAsync(b, context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	ch2 := waitAsync(b, ctx2)
	waitArrived(t, b, 2)

	cancel2()
	if r := recvResult(t, ch2); !errors.Is(r.err, context.Canceled) {
		t.Fatalf("cancelled Wait err = %v, want context.Canceled", r.err)
	}
	waitArrived(t, b, 1)

	// Two new arrivals fill the freed slot and trip generation 0.
	ch3 := waitAsync(b, context.Background())
	waitArrived(t, b, 2)
	ch4 := waitAsync(b, context.Background())
	for _, ch := range []<-chan waitResult{ch1, ch3, ch4} {
		r := recvResult(t, ch)
		if r.err != nil || r.gen != 0 {
			t.Errorf("Wait = (%d, %v), want (0, nil)", r.gen, r.err)
		}
	}
	if s := b.Snapshot(); s.Generation != 1 || s.Arrived != 0 {
		t.Errorf("Snapshot = %+v, want Generation=1 Arrived=0", s)
	}
}

func TestConcurrentCancelAndLastArrival(t *testing.T) {
	for i := 0; i < 50; i++ {
		b := mustNew(t, 2)
		ctx1, cancel1 := context.WithCancel(context.Background())
		ch1 := waitAsync(b, ctx1)
		waitArrived(t, b, 1)

		ctx2, cancel2 := context.WithCancel(context.Background())
		ch2 := waitAsync(b, ctx2)
		cancel1() // races with the second (final) arrival

		r1 := recvResult(t, ch1)
		if r1.err == nil {
			// The final arrival won the race and tripped generation 0.
			if r1.gen != 0 {
				t.Fatalf("iter %d: first Wait gen = %d, want 0", i, r1.gen)
			}
			r2 := recvResult(t, ch2)
			if r2.err != nil || r2.gen != 0 {
				t.Fatalf("iter %d: second Wait = (%d, %v), want (0, nil)", i, r2.gen, r2.err)
			}
			if s := b.Snapshot(); s.Generation != 1 || s.Arrived != 0 {
				t.Fatalf("iter %d: Snapshot = %+v, want Generation=1 Arrived=0", i, s)
			}
		} else {
			// The cancellation won; the second waiter is still parked.
			if !errors.Is(r1.err, context.Canceled) {
				t.Fatalf("iter %d: first Wait err = %v, want context.Canceled", i, r1.err)
			}
			cancel2()
			r2 := recvResult(t, ch2)
			if !errors.Is(r2.err, context.Canceled) {
				t.Fatalf("iter %d: second Wait err = %v, want context.Canceled", i, r2.err)
			}
			if s := b.Snapshot(); s.Generation != 0 || s.Arrived != 0 {
				t.Fatalf("iter %d: Snapshot = %+v, want Generation=0 Arrived=0", i, s)
			}
		}
		cancel1()
		cancel2()
	}
}

func TestSnapshot(t *testing.T) {
	b := mustNew(t, 3)
	s := b.Snapshot()
	if want := (Snapshot{Generation: 0, Arrived: 0, Parties: 3, Closed: false}); s != want {
		t.Errorf("initial Snapshot = %+v, want %+v", s, want)
	}

	ch1 := waitAsync(b, context.Background())
	waitArrived(t, b, 1)
	if s := b.Snapshot(); s.Generation != 0 || s.Arrived != 1 || s.Parties != 3 || s.Closed {
		t.Errorf("Snapshot = %+v, want Generation=0 Arrived=1 Parties=3 Closed=false", s)
	}

	// The returned Snapshot is an independent copy.
	s.Arrived = 99
	if got := b.Snapshot().Arrived; got != 1 {
		t.Errorf("mutating a Snapshot changed the barrier: Arrived = %d, want 1", got)
	}

	ch2 := waitAsync(b, context.Background())
	ch3 := waitAsync(b, context.Background())
	for _, ch := range []<-chan waitResult{ch1, ch2, ch3} {
		recvResult(t, ch)
	}
	if s := b.Snapshot(); s.Generation != 1 || s.Arrived != 0 {
		t.Errorf("Snapshot = %+v, want Generation=1 Arrived=0", s)
	}

	b.Close()
	if s := b.Snapshot(); !s.Closed {
		t.Errorf("Snapshot.Closed = false after Close, want true")
	}
}

func TestClose(t *testing.T) {
	b := mustNew(t, 3)
	ch1 := waitAsync(b, context.Background())
	ch2 := waitAsync(b, context.Background())
	waitArrived(t, b, 2)

	b.Close()
	for _, ch := range []<-chan waitResult{ch1, ch2} {
		if r := recvResult(t, ch); !errors.Is(r.err, ErrClosed) {
			t.Errorf("waiting Wait err = %v, want ErrClosed", r.err)
		}
	}
	if _, err := b.Wait(context.Background()); !errors.Is(err, ErrClosed) {
		t.Errorf("Wait after Close err = %v, want ErrClosed", err)
	}
	if s := b.Snapshot(); !s.Closed || s.Arrived != 0 {
		t.Errorf("Snapshot = %+v, want Closed=true Arrived=0", s)
	}
	b.Close() // idempotent: must not panic or close a channel twice
}

func TestConcurrentGenerations(t *testing.T) {
	const (
		parties = 4
		rounds  = 5
	)
	b := mustNew(t, parties)
	gens := make([][]uint64, parties)
	errs := make([][]error, parties)
	var wg sync.WaitGroup
	for i := 0; i < parties; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				gen, err := b.Wait(context.Background())
				gens[idx] = append(gens[idx], gen)
				errs[idx] = append(errs[idx], err)
			}
		}(i)
	}
	wg.Wait()
	for i := 0; i < parties; i++ {
		for r := 0; r < rounds; r++ {
			if errs[i][r] != nil || gens[i][r] != uint64(r) {
				t.Errorf("goroutine %d round %d: Wait = (%d, %v), want (%d, nil)",
					i, r, gens[i][r], errs[i][r], r)
			}
		}
	}
	if s := b.Snapshot(); s.Generation != rounds || s.Arrived != 0 {
		t.Errorf("Snapshot = %+v, want Generation=%d Arrived=0", s, rounds)
	}
}
