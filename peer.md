# peerd peer 连接问题调查记录

## 背景

本次调查的问题是：同一台机器上，HL node 可以连接公网 peer 并同步区块，但 hypersync 的 peerd 一度无法发现可用 live peer。

需要确认的是：

- peerd 是否握手协议不对。
- peerd 所在 Docker 网络是否无法连外部 peer。
- node 能连而 peerd 不能连，差异是否来自 peer 发现来源。

## 现象

peerd 之前连续多轮探测为空池：

```text
candidates=48 live=0 round_tip=None pruned=0 kept_previous=false probe_concurrency=8 empty_streak=1 sleep=600s
reasons={"connect_error": 1, "connect_timeout": 7, "header_eof": 33, "header_timeout": 6, "peer_full": 1}

candidates=48 live=0 round_tip=None pruned=0 kept_previous=false probe_concurrency=8 empty_streak=2 sleep=1200s
reasons={"connect_error": 1, "connect_timeout": 7, "header_eof": 35, "header_timeout": 4, "peer_full": 1}
```

同期把 HL node 临时切到 direct root 配置后，node 可以正常发现公网 peer 并同步：

```text
connected to abci stream from 52.69.52.236:4001
received abci greeting from 52.69.52.236:4001
connecting to peer: Ip(64.31.48.126)
received abci greeting from 64.31.48.126:4001
successfully received greeting from peer, send_abci=false Ip(64.31.48.126)
got 100 client blocks ... self.rpc_node_ip: Ip(64.31.48.126)
applied block 1060237800
applied block 1060237900
```

## 对照抓包结果

选择 node 实际连接成功的 peer：`64.31.48.126:4001`。

peerd 从容器网络发起连接：

```text
172.28.0.128:<ephemeral> -> 64.31.48.126:4001
```

经宿主机 NAT 后表现为：

```text
152.53.128.11:<ephemeral> -> 64.31.48.126:4001
```

peerd 发送的 greeting：

```text
00000003 00000000
```

对端返回接受帧：

```text
00000001 0003
```

随后对端开始推送 type=1 payload：

```text
00007bce 01...
```

这说明：

- peerd 到公网 peer 的 TCP 连接可以建立。
- peerd 发送的 greeting 与 node 使用的 `send_abci=false` 连接语义一致。
- 对端接受 peerd 的连接，并返回数据帧。
- peerd 的握手实现不是本次 live peer 为空的根因。

抓包还观察到 `64.31.48.126` 会反向连接本机 `4001`：

```text
64.31.48.126:<ephemeral> -> 152.53.128.11:4001
```

当 direct HL node 正在运行并暴露 `4001` 时，这个反连会进入 node 容器。这与 HL gossip 的双向连接行为一致。

## 候选池验证

把 HL node 实际发现过的 peer 加入 peerd 的 `data/peer_candidates.txt` 后，peerd 立即恢复 live peer 输出。

加入的代表性 peer：

```text
64.31.48.126
52.69.52.236
64.31.48.111
72.46.86.185
18.182.127.139
52.198.4.11
54.178.38.245
64.34.94.155
89.167.96.38
109.109.164.200
116.199.229.233
139.180.205.11
74.63.207.101
```

重新启动 peerd 后结果：

```text
candidates=59 live=17 round_tip=Some(1355959416) pruned=0 kept_previous=false
probe_concurrency=8 empty_streak=0 sleep=300s
candidate_fallback=false
top=["95.217.83.242", "64.34.94.155", "52.69.52.236", "18.182.127.139", "64.31.48.126", "142.91.108.10"]
```

清理候选文件中一条由 append 无换行导致的坏行后，再次验证：

```text
candidates=59 live=19 round_tip=Some(1355962971) pruned=0 kept_previous=false
probe_concurrency=8 empty_streak=0 sleep=300s
candidate_fallback=false
```

当前 `peers.json` 中包含：

```json
{
  "live_servers": [
    "18.176.100.70",
    "104.156.238.7",
    "64.34.94.155",
    "54.178.38.245",
    "18.182.127.139",
    "142.91.109.242",
    "82.192.69.246",
    "18.182.166.26",
    "74.63.207.101",
    "142.91.108.10",
    "188.40.215.239",
    "89.167.96.38",
    "64.31.48.126",
    "52.69.52.236",
    "95.217.83.242",
    "206.223.226.137",
    "109.109.164.200",
    "142.91.106.197",
    "46.225.75.91"
  ],
  "n_candidates": 59,
  "round_tip": 1355962971,
  "candidate_fallback": false
}
```

## 根因判断

本次问题的根因不是 peerd 无法连接 peer。后续验证修正了最初判断：候选 peer 来源不够新是问题之一，但不是唯一主因，也不像是这次 `live=0` 反复出现的主因。

具体表现：

- peerd 原候选池里大量 peer 对 probe 的表现是 `header_eof`、`header_timeout`、`connect_timeout` 或 `peer_full`。
- direct HL node 可以通过真实 gossip/root peer discovery 找到新的可服务 peer，例如 `64.31.48.126`、`52.69.52.236`。
- peerd 一旦使用 node 发现的新 peer，马上可以得到 live peer 列表。
- 但同一批新候选在 node 停止后又恢复成大面积 `header_eof`，说明不能只用“候选过旧”解释。
- 重新启动 node、保持候选池不变后，peerd 又恢复 live peer。这更支持一个操作层面的结论：peerd 的 probe 结果依赖本机公网 `4001` 可被 peer 反连，或者至少依赖同一 host 上有 node 对外服务 `4001`。

因此差异不是 TCP、Docker 网络、NAT、4001 握手或 peerd greeting 格式；主要差异更可能是 HL peer 对 dialer 的反连/reciprocity 行为，以及 peerd 候选发现刷新不足这两个因素叠加。

## 本次临时处理

已完成：

- 使用 direct HL node 发现公网可用 peer。
- 抓包确认 peerd 到 `64.31.48.126:4001` 的握手和响应正常。
- 将 node 发现的可用 peer seed 到 `data/peer_candidates.txt`。
- 清理候选文件中一条拼接坏的非法 IP 行：`74.63.207.10164.31.48.126`。
- 重启 peerd 并确认恢复到 `live=19`。
- 停止 direct HL node。
- 恢复 HL node 配置为 gateway-only：

```json
{"root_node_ips": [{"Ip": "172.28.0.10"}], "try_new_peers": false, "chain": "Mainnet"}
```

当前容器状态：

```text
hypersync-peerd        Up
hypersync-gw           Exited
hyperliquid-node-1     Exited
hyperliquid-node-2     Exited
```

## 后续建议

短期：

- 在启动 gateway 测试前，确认 `data/peers.json` 的 `live_servers` 非空。
- 如果 live peer 再次归零，先检查 `data/peer_candidates.txt` 是否过旧，而不是优先怀疑 peerd 握手。
- 保留 peerd 的失败原因统计日志，重点看 `header_eof`、`header_timeout`、`peer_full` 的比例变化。

长期：

- peerd 需要更可靠的 peer discovery/candidate refresh 机制。
- 仅依赖 `gossipRootIps` 和历史候选池不够稳定，候选池会过期。
- 推荐让 peerd 自己实现或复用 HL gossip peer discovery/query peers 流程，避免必须借助 direct HL node seed 候选。
- 也可以考虑把 gateway 成功使用过的 live peer 持久化回候选池，作为补充刷新来源。

## 首次启动结论

如果是一台全新的机器，既没有 `data/bootstrap.cache`，也没有历史 `data/peer_candidates.txt`
或 `data/peers.json`，并且本机还没有任何 HL node 对公网提供 `4001`，那么当前实现不能保证
peerd 第一次就能探测出稳定 live peer pool。

原因是 peerd 虽然会调用 `gossipRootIps` 获取候选 peer，但本次抓包显示部分公网 peer 会在接受
`send_abci=false` 连接后反连本机 `4001`。如果本机没有一个真实 HL node 或等价服务响应这个反连，
这些 peer 可能直接 EOF，导致 peerd 看起来 `live=0`。

当前可靠首启方式：

1. 先启动 peerd + gateway，并观察 `data/peerd.log` 和 `data/peers.json`。
2. 如果 `live_servers` 能自然变为非空，并且 gateway 成功生成 `data/bootstrap.cache`，则可以直接让
   HL node 走 gateway。
3. 如果 `live_servers` 持续为 0，则临时启动一个 direct/public HL node，让它直接连接 HL root peer，
   暴露本机 `4001`，并把它发现的 peer 写入 `data/peer_candidates.txt`。
4. 等 peerd 恢复到稳定 live pool，并且 gateway 已经捕获并持久化 bootstrap cache 后，再把 node
   切回 gateway-only 配置。

这不是理想产品语义。长期应该补一个明确的 bootstrap mode，例如：

- 支持配置一组可信 seed peers，首启时用于填充 candidate/live pool。
- 让 peerd 复用 HL gossip peer discovery，而不是只依赖 `gossipRootIps` 和历史候选。
- 让 gateway 成功连过的上游 peer 反写到 `peer_candidates.txt`，避免候选池只靠 node 外部刷新。
- 如果要让 gateway 自己承担公网 `4001` reciprocity，需要额外做 allowlist、限流和协议语义校验；
  当前 gateway 的 4000-4010 listener 是给下游 node 用的，不能直接视为安全的公网 bootstrap 服务。

---

## Review comments (Claude, 2026-07-04 21:0x, appended after checking live state)

### 1. The handshake analysis is sound — agreed

The capture evidence is conclusive on what it claims: greeting bytes `00000003 00000000` match
`GREET_FALSE`, the `00000001 0003` accept frame and subsequent type=1 data prove peerd's
handshake, Docker network, and NAT path are all fine. Ruling those out is solid work.

### 2. The root-cause verdict ("candidates too stale") is only partially supported — the log evidence points at a stronger alternative

Three observations from this same investigation contradict the strong form of the staleness
conclusion:

- **10 of the 19 recovered live peers are old-pool members** (95.217.83.242, 142.91.109.242,
  82.192.69.246, 18.176.100.70, 104.156.238.7, 206.223.226.137, 142.91.106.197, 46.225.75.91,
  142.91.108.10, 74.63.207.101). If the old candidates were stale/dead, they would not probe
  live minutes after reseeding.
- **The collapse recurred with the fresh pool**: peerd.log 12:46:06 and 12:56:46 show live=0
  with header_eof=43 — six minutes after two healthy live=19 cycles (12:38, 12:40) over largely
  the same candidates. Candidate staleness cannot flip 43 peers in six minutes.
- **Timeline correlation**: hyperliquid-node-1 exited (137) at 12:42:00Z — between the last
  healthy cycle (12:40) and the first collapsed one (12:46). Your own capture shows peers
  dialing BACK to this host's 4001 after accepting a connection. Working hypothesis:
  **HL peers reciprocity-check the dialer** — when nothing on this host serves inbound 4001
  (node stopped), peers accept the TCP connection, read the greeting, and close (= header_eof
  en masse). This explains (a) the mass flip at 12:46, (b) why the same candidate pool probed
  live=9-12 for days while the node was up, (c) why the earlier empty streaks coincided with
  the test window when gw/node containers were down, and (d) why reseeding at 12:38 "worked" —
  the direct node was running at that moment.

**Falsifiable test**: start node-1, change nothing else, run one probe cycle. If live recovers
without touching peer_candidates.txt, reciprocity is the dominant cause and candidate freshness
is secondary. (Genuine discovery weakness is still real — the node did find peers the API never
surfaced — so the long-term recommendations stand either way.)

Also note `peer_full=23-24` of ~59 candidates in the healthy cycles: nearly half the pool
rejects for capacity. Today's testing (full-pool parallel `select_live_peer` dials on every
4001 connect, repeated probe cycles, node bootstraps) all originates from one host IP; keeping
aggregate connection pressure down is worth designing for regardless of root cause.

### 3. This incident empirically confirms two findings from review.md

- The early `live=0 ... kept_previous=false` lines in this document are the review's
  "deploy-transition landmine" firing in practice: the old-format peers.json (no `round_tip`)
  was untrusted, the candidate-fallback safety net is removed, so empty pools were written over
  a previously good file. The 12:46+ cycles show `kept_previous=true` protecting the new-format
  file — the keep-on-collapse mechanism works once trusted. Conclusion: stage the format
  transition (treat old-format pools as trusted for keep purposes, and never overwrite a
  non-empty pool with an empty one).
- The probe failure-reason histogram (`reasons={...}`, `samples=[...]`) is what made this
  diagnosable at all — it must stay in the final change.

### 4. Smaller notes

- The corrupted candidate line `74.63.207.10164.31.48.126` came from a manual append to a file
  whose last line lacks a trailing newline (`write_atomic` joins with `\n` and writes no final
  newline). It then survived into probe targets (12:38 sample shows `connect_error` on the
  merged token). Two cheap hardenings: end `write_atomic` content with a newline, and validate
  candidate lines through the existing `extract_ipv4`/`is_ipv4` on read.
- Operational state at time of writing: hypersync-gw Exited(137) ~2h, hyperliquid-node-1
  Exited(137) 12:42Z, hyperliquid-node-2 Exited(137) ~3h — **production sync is currently
  down**. Also `hypersync:latest` was rebuilt at 20:11 from the current working tree, so any
  container restart now picks up the unreviewed build; the last reviewed+soaked build is git
  899b240. Decide which build to run before bringing the stack back up.
- The working tree changed after review.md's snapshot (+467/−292 → +770/−346; peerd.rs edited
  20:10). review.md's findings reference the earlier snapshot — several may already be
  addressed, others (pump_merge silent drops, select_live_peer no-fallback, 4002 strict-active
  starvation) likely still apply. A re-review pass against the current tree is warranted before
  any commit.

### 5. On the long-term recommendations — endorsed, with additions

Implementing/reusing the HL gossip query-peers flow and persisting gateway-served live peers
back into the candidate pool are both right. Additions: harvest from the node's own
`tcp_lz4_stats` when co-located (the compose colocated overlay already mounts it); and if the
reciprocity hypothesis in §2 holds, document that probe results are only meaningful while a
node on this host is serving inbound 4001 — or run probes with that constraint in mind.

---

## Codex verification (2026-07-04 21:07 CST)

I checked the appended review comments against the current logs, code, and one live experiment.

### Verified

- The handshake conclusion is correct. `GREET_FALSE` is defined as `[0, 0, 0, 3, 0, 0, 0, 0]` in `src/protocol.rs`, matching the captured `00000003 00000000`. The captured accept/status frame and subsequent type=1 payload still support the conclusion that TCP, Docker NAT, and greeting format are not the failure.
- The "candidate staleness only" conclusion was too strong. `peerd.log` shows healthy cycles with the fresh pool:

```text
12:38:56 candidates=60 live=19 ... header_eof=1 ... peer_full=23
12:40:26 candidates=59 live=19 ... header_eof=2 ... peer_full=24
```

Then, with the same cleaned candidate pool and after node was stopped, peerd collapsed:

```text
12:46:06 candidates=59 live=0 ... kept_previous=true ... header_eof=43
12:56:46 candidates=59 live=0 ... kept_previous=true ... header_eof=43
```

- I ran the falsifiable test proposed in the comment: without changing `data/peer_candidates.txt`, I started `hyperliquid-node-1` so host `4001` was listening, then restarted peerd for one probe cycle. Node was still unable to bootstrap because gw was down, but it did expose `4000-4010`.
- Result: peerd recovered immediately:

```text
13:07:04 candidates=59 live=18 round_tip=Some(1355986622) kept_previous=false
reasons={"connect_error": 1, "connect_timeout": 7, "header_eof": 2, "header_timeout": 9, "payload_timeout": 16, "peer_full": 24}
```

This strongly supports the reciprocity hypothesis at the operational level: peerd probe results are meaningful only when this host is also serving inbound `4001`, or at least when an HL node is listening there.

- The bad candidate line explanation is correct. `write_atomic` writes `join("\n")` without a trailing newline, while peerd initially loads `peer_candidates.txt` via `s.lines().map(str::to_string)`, not through `extract_ipv4`. A manual append can therefore create a merged token like `74.63.207.10164.31.48.126`, and that token can survive into probe targets.
- The current container state is still not a running sync stack:

```text
hypersync-peerd        Up
hypersync-gw           Exited
hyperliquid-node-1     Exited
hyperliquid-node-2     Exited
```

- `hypersync:latest` is the local rebuilt image from `2026-07-04T20:11:04+08:00` (`sha256:13a3209...`), while current git `HEAD` is `89eadaa` and the last referenced reviewed/soaked commit `899b240` is one commit behind `HEAD`. The current working tree also has large uncommitted changes in `src/gateway.rs`, `src/peerd.rs`, `src/protocol.rs`, and `src/push.rs`.

### Correction to the appended comment

The statement that an old-format `peers.json` without `round_tip` is currently untrusted is not accurate for the current working tree. Current code uses:

```rust
fn previous_peer_pool_is_trusted(contents: &str) -> bool {
    !contents.contains("\"candidate_fallback\":true") && !extract_ipv4(contents).is_empty()
}
```

and the test explicitly treats an old-format non-empty pool as trusted:

```rust
assert!(previous_peer_pool_is_trusted(
    r#"{"live_servers":["1.1.1.1"],"n_candidates":48}"#
));
```

The deploy-transition risk still exists historically and should stay in review discussion, but it should be phrased as a risk from earlier snapshots/format transitions, not as the behavior of the current tree.

### Follow-up fixes implied by verification

- Add a trailing newline when peerd writes `peer_candidates.txt`.
- Validate candidate file lines on read using `extract_ipv4`/`is_ipv4` instead of accepting raw `lines()`.
- Document or enforce the probe precondition: peerd's live-probe signal depends on this host serving inbound `4001`.
- Re-review current uncommitted code before commit, because `review.md` references an earlier snapshot and the current diff is materially larger.
