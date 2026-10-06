# cyclicbarrier

Go 实现的并发安全循环屏障（Cyclic Barrier），仅含库、最小 CLI 演示和自动化测试，零第三方依赖。

## 功能

- `NewBarrier(parties)`：创建屏障，要求 `parties > 0`，否则返回错误
- `Wait(ctx)`：加入当前 generation；凑齐 `parties` 后，该代全部参与者同时放行并返回 generation 编号，随后自动进入下一代
- `Snapshot()`：返回 `Generation / Arrived / Parties / Closed` 的独立副本
- `Close()`：唤醒当前所有等待者（返回 `ErrClosed`），并拒绝后续 `Wait`；幂等

## 语义

- generation 从 0 开始编号，不同 generation 严格隔离
- `Wait` 的 context 取消只撤回自身，不会错误触发 barrier；取消与最后一个参与者到达并发时二者必居其一，计数一致、无重复返回、无死锁
- `Snapshot` 返回值是状态副本，修改它不影响 barrier

## 使用

```go
b, _ := cyclicbarrier.NewBarrier(3)
gen, err := b.Wait(context.Background()) // 凑齐 3 人后全部返回 gen=0
s := b.Snapshot()
b.Close()
```

## 运行演示

```
go run ./cmd/barrierdemo
```

演示两轮 barrier、一次 context 取消和 Close。

## 测试

```
go test -race ./...
```

## 非目标

动态调整 parties、分布式 barrier、持久化、网络、GUI、数据库。
