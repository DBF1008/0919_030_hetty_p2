#!/usr/bin/env bash
#
# test.sh — 手动运行全部单元测试脚本
#
# 覆盖本次改造涉及的包:
#   - pkg/reqlog      (RequestModifier/ResponseModifier、io.LimitReader 保护)
#   - pkg/db/bolt     (批量缓冲写入、单事务提交、失败重试)
#   - pkg/sender      (依赖 reqlog 存储的回归测试)
# 以及其余所有包的单元测试。
#
# 用法:
#   ./test.sh
#
# 注意: 需要 Go 工具链 (见 go.mod 的 go 版本)。若默认 GOCACHE/GOMODCACHE
# 不可写, 可先执行:
#   export GOCACHE=/tmp/gocache GOMODCACHE=/tmp/gomodcache

set -euo pipefail

cd "$(dirname "$0")"

export CGO_ENABLED=0

echo "==> [1/5] go build ./pkg/..."
go build ./pkg/...

echo "==> [2/5] go vet ./pkg/..."
go vet ./pkg/...

echo "==> [3/5] 单元测试: pkg/reqlog (含竞态检测, 详细输出)"
go test -race -count=1 -v ./pkg/reqlog/...

echo "==> [4/5] 单元测试: pkg/db/bolt + pkg/sender (含竞态检测, 详细输出)"
go test -race -count=1 -v ./pkg/db/bolt/... ./pkg/sender/...

echo "==> [5/5] 全量单元测试: ./pkg/..."
go test -count=1 ./pkg/...

echo "==> 全部测试通过 ✅"
