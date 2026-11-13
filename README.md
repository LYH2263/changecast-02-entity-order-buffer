# 04-changecast（变投影）

Changecast — 变更日志投影：源变更流 → 按实体键局部保序 → 乱序缓冲/逾限阻塞 → 快照与批次互证。

与 Feedstitch（多源 Feed 归并）、Batchgate（作业依赖编排）、Syncledger（双端对账）主链路不同。夹具全本地，不连活库。

## 启动

```bash
docker compose up --build
```

| 入口 | 地址 |
| --- | --- |
| 前端 | http://localhost:5400 |
| API | http://localhost:10400 |

本地开发：`cd backend && uvicorn app.main:app --port 10400`；测试：`cd backend && pytest`。

## 主链

配置投影流 → 按**到达顺序**读取 CDC 夹具 → 逐实体保序闸门（seq 命中期望才改投影）→ 超前入可审计缓冲 → 逾限阻塞该实体 → 清缓冲/补齐后连续放出 → 批次计数与快照互证。

## 实体保序语义（本切口已实现 arrival_vs_event_order）

- seq 以 **(流, 实体键)** 为作用域。同一实体上仅当事件 seq **严格等于**该实体期望的下一个 seq（新实体为流配置的 `start_seq`）才立刻改投影。
- `seq > 期望` → 进按实体分组的可审计缓冲（状态 open），**不得改投影**；`seq < 期望` → 迟到低 seq，判 `skipped_stale`，绝不回盖已应用的高 seq（存储层 upsert 亦带 seq 守卫）。
- 实体之间互不影响：某一实体缓冲/阻塞时，其他实体照常应用，不整批卡死。
- 运行是**增量**的：投影、各实体期望 seq、缓冲与阻塞态跨批次保持；重跑同一夹具只得 `skipped_duplicate`。

## 逾限策略（二选一，已选定并贯彻）

**`block`（阻塞该实体）**：实体缓冲条数 > `buffer_max_events`，或最老缓冲等待 ≥ `buffer_max_wait_seconds`（均按流可配），该实体即被阻塞——已缓冲条目转 held，后续乱序事件记 `held_blocked` 滞留，**不丢弃任何事件**（`dropped_overflow` 恒为 0，仅作策略对称留痕）。解除路径两条：

1. **补齐缺口**：后续运行带来 `seq == 期望` 的事件（如 `reorder_lab_fill.json`），即应用并把缓冲按实体内 seq 连续放出，排空后自动解除阻塞。
2. **人工清缓冲**：`POST /api/ooo-buffer/flush`（缓冲区页有按钮）——显式允许缓冲内最小 seq 为新起始 seq，缺口计入 `gap_skipped`，再按实体内 seq 连续放出；仍有内缺口则保持阻塞。

## 计数互证

- 运行批：`源事件 = 即刻应用 + 入缓 + 阻塞滞留 + 过期 + 重复 + 冗余`；清缓冲批：`处理数 = 放出 + 过期`。批次表带 `reconciled` 标记。
- 缓冲中 / 已应用 / 阻塞滞留 / 逾限阻塞等计数与缓冲区列表、投影快照、批次报告一致；事件留痕记录每个事件的裁决及「入缓 → 放出」全生命周期。

## 种子投影流

- **member_profile**：会员档案变更（`member_id`）—— 到达序乱序，同批内补齐放出
- **store_archive**：门店档案变更（`store_id`）—— 同上
- **channel_attr**：渠道属性变更（`channel_code`）—— 同上
- **reorder_lab**：乱序演练流（`item_id`，上限 2 条/600s）—— L-A 超前入缓后放出；L-B 逾限阻塞；L-C 不受影响；L-D 挂缺口待人工清缓冲；L-E 迟到低 seq 被拦

## 演示路径

1. 投影台跑 `reorder_lab`（默认夹具）→ 看批次计数与原因。
2. 缓冲区页：L-B 已阻塞（held 4 条）、L-D 缓冲中（缺 seq 2）→ 对 L-D「清缓冲并放行」。
3. 缓冲区页对 L-B「跑补齐夹具」或投影台覆盖 `reorder_lab_fill.json` 再跑 → L-B 连续放出并解除阻塞。
4. 应用批次 / 事件留痕 / 投影快照互证。

## 技术栈

Python 3.12 + FastAPI + SQLite；Vue 3 + Vite + Nginx。

## 0-1 切口

- [x] `arrival_vs_event_order`（按实体键局部保序 + 可审计缓冲 + 逾限阻塞，本切口）
- [ ] `event_idempotency` / `tombstone_policy` / `watermark_rewind` / …（留待后题）
