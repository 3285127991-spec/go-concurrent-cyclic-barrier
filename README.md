# cyclicbarrier

A concurrency-safe cyclic barrier for Go, with zero third-party dependencies.

A barrier is created for a fixed number of parties. Each `Wait` joins the
current generation; once all parties have arrived, every participant of that
generation is released with the generation number and the barrier advances to
the next generation. Generations are strictly isolated.

## API

- `New(parties int) (*Barrier, error)` — parties must be > 0.
- `Wait(ctx context.Context) (uint64, error)` — blocks until the generation
  trips, ctx is done, or the barrier is closed. Returns the completed
  generation number (the first generation is 0).
- `Snapshot() Snapshot` — independent copy of {Generation, Arrived, Parties, Closed}.
- `Close()` — wakes all current waiters with `ErrClosed` and rejects new
  waits. Idempotent.

## Semantics

- A canceled `Wait` withdraws only itself; it never trips the barrier and
  never affects other parties. The freed slot can be refilled.
- If a cancellation races with the last arrival, the generation trip wins and
  the wait reports success, so counts stay consistent with no duplicate
  returns and no deadlock.
- After `Close`, pending and future `Wait` calls return `ErrClosed`.

## Usage

```go
b, _ := cyclicbarrier.New(3)
gen, err := b.Wait(context.Background()) // call from 3 goroutines
s := b.Snapshot()
b.Close()
```

## Run

```
go test -race ./...
go run ./cmd/barrier-demo
```

The demo shows two barrier rounds, one canceled wait, and Close.

## Non-goals

No dynamic parties, no distributed barrier, no persistence, no network,
no GUI, no database.
