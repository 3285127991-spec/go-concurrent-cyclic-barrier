// Command barrier-demo is a minimal demo of the cyclicbarrier package:
// two full barrier rounds, one context cancellation, and Close.
package main

import (
	"context"
	"fmt"
	"sync"

	"cyclicbarrier"
)

func main() {
	b, err := cyclicbarrier.New(3)
	if err != nil {
		panic(err)
	}

	fmt.Println("== two barrier rounds (parties=3) ==")
	var wg sync.WaitGroup
	for id := 0; id < 3; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for round := 0; round < 2; round++ {
				gen, err := b.Wait(context.Background())
				if err != nil {
					fmt.Println("worker", id, "round", round, "failed:", err)
					return
				}
				fmt.Println("worker", id, "released from generation", gen)
			}
		}(id)
	}
	wg.Wait()

	fmt.Println("== cancel a single wait ==")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := b.Wait(ctx); err != nil {
		fmt.Println("wait returned:", err)
	}
	fmt.Println("snapshot after cancel:", fmt.Sprintf("%+v", b.Snapshot()))

	fmt.Println("== close ==")
	b.Close()
	if _, err := b.Wait(context.Background()); err != nil {
		fmt.Println("wait after close:", err)
	}
	fmt.Println("final snapshot:", fmt.Sprintf("%+v", b.Snapshot()))
}
