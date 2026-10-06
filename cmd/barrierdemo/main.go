// Command barrierdemo demonstrates the cyclicbarrier package: two barrier
// rounds, one context cancellation, and Close.
package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"

	"cyclicbarrier"
)

func main() {
	b, err := cyclicbarrier.NewBarrier(3)
	if err != nil {
		panic(err)
	}

	// Two full barrier rounds with three workers each.
	for round := 0; round < 2; round++ {
		var wg sync.WaitGroup
		for id := 0; id < 3; id++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				gen, err := b.Wait(context.Background())
				fmt.Printf("round %d: worker %d released in generation %d (err=%v)\n", round, id, gen, err)
			}(id)
		}
		wg.Wait()
	}

	// One waiter cancels: it withdraws without tripping the barrier.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := b.Wait(ctx)
		done <- err
	}()
	for b.Snapshot().Arrived == 0 {
		runtime.Gosched()
	}
	cancel()
	fmt.Println("cancelled waiter returned:", <-done)
	fmt.Println("arrived after cancel:", b.Snapshot().Arrived)

	// Close wakes the parked waiter and rejects new arrivals.
	go func() {
		_, err := b.Wait(context.Background())
		done <- err
	}()
	for b.Snapshot().Arrived == 0 {
		runtime.Gosched()
	}
	b.Close()
	fmt.Println("waiter woken by Close:", <-done)
	if _, err := b.Wait(context.Background()); err != nil {
		fmt.Println("Wait on closed barrier:", err)
	}
	fmt.Println("final snapshot:", b.Snapshot())
}
