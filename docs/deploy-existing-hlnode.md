# hypersync 部署到已有 HL node 服务器

本文档用于把 `hypersync` 部署到一台已经在运行 Hyperliquid HL node 的服务器上。

目标拓扑：

```text
HL node container  ->  hypersync-gw:4000-4010  ->  public HL peers
                         ^
                         |
                    hypersync-peerd
```

`hypersync-gw` 和 `hypersync-peerd` 独立作为一个 Docker Compose 项目运行。已有 HL node 不需要放进
hypersync 的 compose 里，但需要加入 `hypersync_gwnet` 网络，并把
`override_gossip_config.json` 指向 gateway。

## 1. 关键结论

- 先保持已有 HL node 正常运行，不要一开始就停掉或切到 gateway-only。
- 先启动 `peerd + gw`，让 peerd 读取已有 HL node 的 `tcp_lz4_stats`，建立 live peer pool。
- 等 `data/peers.json` 有稳定 `live_servers`，并且 `gw` 生成 `data/bootstrap.cache` 后，再切换 HL node。
- 推荐把 gw/peerd 部署在至少一个真实 HL node 所在机器，或者至少保证 gw 出公网的源 IP 和一个健康 HL node 的公网 peer 行为一致。公网 HL peers 可能根据源 IP 是否有正常 `4001/4002` 行为来降级连接；如果 gw 放在一台没有 HL node 公网端口行为的空机器上，peerd/gw 可能拿不到稳定 live peer。
- gateway 默认只在 Docker 内网 `hypersync_gwnet` 暴露。同机部署时不要把 gateway 的 `4000-4010` 直接暴露到公网；异机 node 访问 gw 时，也只应发布到私网/VPN IP 并用防火墙只允许 HL node 来源 IP。
- HL node 自己的公网 `4001/4002` 可以保持原状；这些端口属于 HL node，不是 gateway。
- 多个 HL node 可以共用一个 gateway，但 gateway 按 node 的来源 IP 区分 session。共用 gateway 的多个 node 必须在 gateway 看来拥有不同来源 IP，例如不同 Docker container IP。
- 多 node 同机测试时，内存压力主要来自 HL node 自身，不是 gateway。gateway 常驻通常接近一个 bootstrap cache 的大小，cache refresh 时会短暂升高；每个 HL node 可能到 20GB+，两节点机器要预留足够 RAM/swap。

## 2. 前置条件

服务器上已经满足：

- Docker / Docker Compose 可用。
- HL node 已经运行并且能正常同步。
- 如果 gw/peerd 和 HL node 在同一台 Docker host，HL node 的数据 volume 可被 hypersync 只读挂载，用于读取：

```text
tcp_lz4_stats/
```

- 如果 gw/peerd 在另一台机器，不能直接挂载 HL node volume；需要把 `tcp_lz4_stats/` 目录复制或定时同步到 gw/peerd 机器上的某个路径，再把这个路径只读挂载给 peerd。
- 异机 gw 还有一个额外前提：gw 机器的公网出口 IP 最好本身也有真实 HL node 的 `4001/4002` 行为，或通过网络/NAT 让公网 peers 看到的是已有 HL node 的同一个健康出口 IP。只复制 `tcp_lz4_stats` 能改善候选发现，但不能解决公网 peer 对 gw 源 IP 的互惠/降级判断。
- `tcp_lz4_stats` 是一个目录，里面通常是一组按日期命名的文件，例如 `20260706`；不要只复制单个文件，至少要复制整个目录。它只给 `peerd` 用，`gw` 不读 HL node 文件。

默认示例假设 HL node volume 名称是：

```text
hyperliquid_hl-data
```

如果你的实际 volume 名称不同，需要修改 `docker-compose.colocated.yml`。

查询现有 HL node 容器和 volume：

```sh
docker ps
docker volume ls | grep -i hyper
docker inspect <HL_NODE_CONTAINER> --format '{{json .Mounts}}'
```

查询服务器公网 IP：

```sh
curl -4 ifconfig.me
```

后面会把这个公网 IP 写入 `HL_SELF_IP`，防止 peerd 把自己的 HL node 当作上游 peer。

## 3. 拉代码并构建镜像

```sh
cd /opt
git clone git@github.com:bufrr/hypersync.git
cd hypersync
git checkout main
mkdir -p data
docker compose build
```

如果仓库已经存在：

```sh
cd /opt/hypersync
git pull
docker compose build
```

## 4. 配置 `.env`

在 hypersync repo 根目录创建 `.env`：

```sh
cat > .env <<'EOF'
HL_SELF_IP=<YOUR_PUBLIC_NODE_IP>
EOF
```

多个 HL node 公网 IP 用逗号分隔：

```sh
HL_SELF_IP=1.1.1.1,2.2.2.2
```

`HL_SELF_IP` 的作用是：这些 IP 不会进入 gateway 的上游 peer pool，避免 gateway 把自己的 node
互相转发成上游。

## 5. 确认 HL node 数据 volume

先在 HL node 容器里找到 `tcp_lz4_stats` 的实际位置：

```sh
docker exec <HL_NODE_CONTAINER> sh -lc 'find /home/hluser/hl -maxdepth 5 -type d -name tcp_lz4_stats -print'
docker exec <HL_NODE_CONTAINER> sh -lc 'ls -lah /home/hluser/hl/data/tcp_lz4_stats | tail'
```

如果实际路径不是 `/home/hluser/hl/data/tcp_lz4_stats`，后续命令里的路径按实际输出替换。

再确认这个目录来自哪个 Docker volume 或 host bind mount：

```sh
docker inspect <HL_NODE_CONTAINER> --format '{{json .Mounts}}'
```

常见情况是容器内 `/home/hluser/hl/data` 对应一个 Docker volume，例如 `hyperliquid_hl-data`。
那么 `tcp_lz4_stats` 在 volume 根目录下，对 peerd 挂载后会表现为：

```text
/hl/node1/tcp_lz4_stats
```

### 同机部署：直接挂载 HL node volume

默认的 `docker-compose.colocated.yml` 内容假设：

```yaml
volumes:
  node1-hl-data:
    external: true
    name: hyperliquid_hl-data
```

如果实际 volume 不是 `hyperliquid_hl-data`，修改为实际名称：

```yaml
volumes:
  node1-hl-data:
    external: true
    name: <YOUR_HL_DATA_VOLUME>
```

这个 overlay 会把 HL node 的数据 volume 只读挂载到 peerd：

```text
/hl/node1/tcp_lz4_stats
```

peerd 会从这里读取 HL node 实际交换过数据的 peer，作为候选来源。这对首次部署很重要。

此时 `HL_STATS_DIR` 由 `docker-compose.colocated.yml` 设置：

```yaml
services:
  peerd:
    environment:
      HL_STATS_DIR: /hl/node1/tcp_lz4_stats
```

确认 peerd 真正看到了这个目录：

```sh
docker exec hypersync-peerd sh -lc 'echo HL_STATS_DIR=$HL_STATS_DIR; ls -lah /hl/node1/tcp_lz4_stats | tail'
```

### 异机部署：复制或同步 tcp_lz4_stats

如果 gw/peerd 不在 HL node 所在机器，不能直接挂载 Docker volume。推荐在 gw/peerd 机器上准备一个本地目录：

```sh
cd /opt/hypersync
mkdir -p hl-stats/node1/tcp_lz4_stats
```

从 HL node 机器同步整个目录到 gw/peerd 机器。示例：

```sh
rsync -az <HL_NODE_HOST>:/path/to/tcp_lz4_stats/ /opt/hypersync/hl-stats/node1/tcp_lz4_stats/
```

如果只能通过容器取文件，可以先在 HL node 机器上导出：

```sh
docker cp <HL_NODE_CONTAINER>:/home/hluser/hl/data/tcp_lz4_stats ./tcp_lz4_stats
rsync -az ./tcp_lz4_stats/ <GW_HOST>:/opt/hypersync/hl-stats/node1/tcp_lz4_stats/
```

生产上建议用 cron 或 systemd timer 每几分钟同步一次。一次性复制也能帮助首次启动，但后续候选不会继续从 node 新日志更新。

在 gw/peerd 机器上新增一个本地 overlay，例如 `docker-compose.stats.yml`：

```yaml
services:
  peerd:
    environment:
      HL_STATS_DIR: /hl/node1/tcp_lz4_stats
    volumes:
      - ./hl-stats/node1/tcp_lz4_stats:/hl/node1/tcp_lz4_stats:ro
```

启动时带上这个 overlay：

```sh
docker compose -f docker-compose.yml -f docker-compose.stats.yml up -d --build
```

如果暂时没有 `tcp_lz4_stats`，也可以不设置 `HL_STATS_DIR`，peerd 会依赖 `gossipRootIps` API、
已有 `data/peer_candidates.txt` / `data/peers.json` 和主动 probe。缺点是首次冷启动的 peer 发现会弱一些，
更容易出现 live pool 建立慢或 `live=0`。

## 6. 启动 peerd + gateway

已有 HL node 还保持原配置继续运行，先启动 hypersync：

```sh
docker compose -f docker-compose.yml -f docker-compose.colocated.yml up -d --build
```

如果使用上一节的异机复制目录 overlay，则改为：

```sh
docker compose -f docker-compose.yml -f docker-compose.stats.yml up -d --build
```

如果完全不提供 `tcp_lz4_stats`，只启动基础 compose：

```sh
docker compose -f docker-compose.yml up -d --build
```

确认容器运行：

```sh
docker ps --filter name=hypersync
```

查看 peerd 日志：

```sh
docker logs -f hypersync-peerd
```

期望看到类似：

```text
[peerd] candidates=59 live=19 round_tip=Some(...) kept_previous=false ...
```

核心判断：

- `live` 不应长期为 `0`。
- 生产切换前建议 `live >= 4`；低于 4 时 gateway 会先暂缓 cache refresh，接近过期时仍会用可用 peer 强制尝试刷新，避免主动等到 cache 过期。
- `round_tip` 应该是 `Some(...)`。
- `data/peers.json` 里应该有 `live_servers`。

查看 peers 文件：

```sh
cat data/peers.json
```

## 7. 等待 bootstrap cache 生成

gateway 使用 `--cache` 时会捕获完整 bootstrap，生成：

```text
data/bootstrap.cache
```

查看 gateway 日志：

```sh
docker logs -f hypersync-gw
```

期望看到类似：

```text
[gw] bootstrap captured ...
[gw] bootstrap cache persisted to /pd/bootstrap.cache
```

如果日志中偶尔出现类似：

```text
[gw] bootstrap attempt via <peer> failed: state-server too slow ...
```

这通常只是 refresh race 淘汰慢 peer，不等同于 gateway 故障。只要后续能看到 cache persisted、
`data/bootstrap.cache` 的 mtime 持续更新，并且 cache age 不超过 30 分钟，就可以继续观察。

确认 cache 文件：

```sh
ls -lh data/bootstrap.cache
stat data/bootstrap.cache
```

正常大小约 4.5GB。切换 node 前，建议 cache mtime 不超过 30 分钟。

如果 cache 暂时没有生成，但 `peers.json` 已经有稳定 live peer，node 也可以通过透明 fallback
从真实 peer bootstrap；只是首次切换时会更依赖公网 peer 状态。生产部署建议先等 cache 成功。

## 8. 让已有 HL node 加入 hypersync 网络

hypersync compose 会创建 attachable 网络：

```text
hypersync_gwnet
```

如果 HL node 也是 Docker Compose 管理，推荐在 HL node 的 compose override 中加入外部网络。

示例：

```yaml
networks:
  hypersync_gwnet:
    external: true

services:
  <HL_NODE_SERVICE_NAME>:
    networks:
      - default
      - hypersync_gwnet
```

应用 node compose 变更：

```sh
cd <HL_NODE_REPO>
docker compose up -d
```

如果暂时不想改 HL node compose，也可以先把正在运行的 node 容器直接接入网络：

```sh
docker network connect hypersync_gwnet <HL_NODE_CONTAINER>
```

这个方式适合临时验证；长期部署仍建议写进 HL node compose，避免容器重建后丢失网络配置。

确认 HL node 已加入 `hypersync_gwnet`：

```sh
docker inspect <HL_NODE_CONTAINER> --format '{{json .NetworkSettings.Networks}}'
```

同机部署时，HL node 连接 gateway 的地址固定为：

```text
172.28.0.10
```

## 9. 修改 HL node gossip 配置

把 HL node 的 `override_gossip_config.json` 改成只连接 gateway：

```json
{"root_node_ips":[{"Ip":"172.28.0.10"}],"try_new_peers":false,"chain":"Mainnet"}
```

文件位置取决于你的 HL node 部署方式。常见位置是 HL node 容器内的 hluser home：

```text
/home/hluser/override_gossip_config.json
```

示例命令：

```sh
docker exec <HL_NODE_CONTAINER> sh -lc 'cat > /home/hluser/override_gossip_config.json <<EOF
{"root_node_ips":[{"Ip":"172.28.0.10"}],"try_new_peers":false,"chain":"Mainnet"}
EOF'
```

建议修改前先备份旧配置：

```sh
docker exec <HL_NODE_CONTAINER> sh -lc 'cp /home/hluser/override_gossip_config.json /home/hluser/override_gossip_config.json.bak.$(date +%Y%m%d%H%M%S) 2>/dev/null || true'
```

## 10. 重启 HL node

只重启 HL node，不需要重启 gateway：

```sh
cd <HL_NODE_REPO>
docker compose restart <HL_NODE_SERVICE_NAME>
```

或者：

```sh
docker restart <HL_NODE_CONTAINER>
```

观察 node 日志：

```sh
docker logs -f <HL_NODE_CONTAINER>
```

期望看到：

```text
connected to abci stream from 172.28.0.10:4001
received abci greeting from 172.28.0.10:4001
got 100 client blocks ...
applied block ...
```

如果命中 cache，gateway 日志会出现类似：

```text
[gw] [<node-ip>] node cold-start FROM CACHE (... MB), no peer state fetch
```

## 11. 验收检查

切换后至少观察 30 分钟。推荐每 5 分钟检查一次。

gateway / peerd：

```sh
docker logs --since 5m hypersync-gw
docker logs --since 5m hypersync-peerd
docker stats --no-stream hypersync-gw hypersync-peerd
```

HL node：

```sh
docker logs --since 5m <HL_NODE_CONTAINER> 2>&1 | grep -E 'applied block|got [0-9]+ client blocks|new app hashes|ERROR|panic|LimitExceeded|Peer-only|Querying jailed validators|forward_client_blocks'
docker stats --no-stream <HL_NODE_CONTAINER>
```

快速取最新 applied block：

```sh
docker logs --since 10m <HL_NODE_CONTAINER> 2>&1 | grep -Eo 'applied block [0-9]+' | tail -1
```

检查容器是否 OOM 或异常重启：

```sh
docker inspect hypersync-gw hypersync-peerd <HL_NODE_CONTAINER> \
  --format '{{.Name}} restart={{.RestartCount}} oom={{.State.OOMKilled}} status={{.State.Status}}'
```

通过标准：

- HL node 高度持续增长。
- `hypersync-peerd` 的 `live` 不长期为 0；生产切换和 cache 预热阶段建议保持 `live >= 4`。
- `hypersync-gw` 没有持续 `4002 client-block fetch failed`。
- HL node 没有持续 `Peer-only request`、`LimitExceeded`、panic 或 OOM。
- `data/bootstrap.cache` 持续刷新，mtime 不长期超过 30 分钟。
- 冷启动 / catch-up 阶段可能短暂出现 `Querying jailed validators for high round` 或
  `forward_client_blocks no more blocks from reader, reconnecting to a new peer`。如果随后
  `applied block` 继续增长、gateway 重新选择 live peer、双 node 高度差重新收敛，可以视为可恢复事件；如果持续出现或高度停滞，应按故障处理。

查看 cache 更新时间：

```sh
stat -c '%y %s' data/bootstrap.cache
```

## 12. 首次启动异常处理

如果 peerd 长时间 `live=0`：

1. 确认原 HL node 仍在 direct/public 模式运行，并且公网 `4001` 可达。
2. 同机部署时，确认 `docker-compose.colocated.yml` 挂载的是正确的 HL node 数据 volume；异机部署时，确认 `docker-compose.stats.yml` 挂载的是已经复制/同步过来的 `tcp_lz4_stats` 目录。
3. 确认 `HL_STATS_DIR` 指向真实存在的 `tcp_lz4_stats`：

```sh
docker exec hypersync-peerd sh -lc 'ls -lah /hl/node1/tcp_lz4_stats | tail'
```

4. 查看 peerd 候选：

```sh
wc -l data/peer_candidates.txt
tail -50 data/peer_candidates.txt
```

5. 重启 peerd：

```sh
docker restart hypersync-peerd
```

如果仍然为 0，不要切换 HL node 到 gateway-only。先让 HL node 保持 direct 模式，等 peerd 候选恢复。

本次测试中的经验是：部分 HL peer 会反连本机公网 `4001`。因此完全空机器首次启动时，如果没有已有
HL node 维持这个公网行为，peerd 可能出现 `header_eof` 或 `live=0`。在“已有 HL node 的服务器”
上部署时，正确流程就是先保持原 node 运行，等 peerd/gw 热起来，再切 node。

## 13. 多个 HL node 共用一个 gateway

多个 HL node 共用一个 gateway 时：

- 每个 node 都要加入 `hypersync_gwnet`。
- 每个 node 的 `override_gossip_config.json` 都指向 `172.28.0.10`。
- 多个 node 在 gateway 看来必须有不同来源 IP。gateway 使用 TCP source IP 作为 node session key；
  如果两个远端 node 经过同一个 NAT / SNAT 到 gateway，gateway 会把它们当成同一个 node，不能这样部署。
- `HL_SELF_IP` 要包含所有本机/本组 HL node 的公网 IP。
- 如果多个 node 有多个 hl-data volume，可以扩展 `docker-compose.colocated.yml`：

```yaml
volumes:
  node1-hl-data:
    external: true
    name: hyperliquid_hl-data
  node2-hl-data:
    external: true
    name: hyperliquid2_hl-data

services:
  peerd:
    environment:
      HL_STATS_DIR: /hl/node1/tcp_lz4_stats,/hl/node2/tcp_lz4_stats
    volumes:
      - ./data:/pd
      - node1-hl-data:/hl/node1:ro
      - node2-hl-data:/hl/node2:ro
```

## 14. 回滚

如果切换后 HL node 不正常：

1. 恢复旧 gossip 配置：

```sh
docker exec <HL_NODE_CONTAINER> sh -lc 'cp /home/hluser/override_gossip_config.json.bak.<TS> /home/hluser/override_gossip_config.json'
```

2. 重启 HL node：

```sh
docker restart <HL_NODE_CONTAINER>
```

3. 停止 hypersync：

```sh
cd /opt/hypersync
docker compose -f docker-compose.yml -f docker-compose.colocated.yml down
```

回滚后确认 HL node 重新连接公网 peer 并继续 applied block。

## 15. 常见问题

### gateway 要不要 host network？

同机部署不需要。推荐使用 Docker bridge 网络 `hypersync_gwnet`，HL node 通过 `172.28.0.10`
访问 gateway。

如果 gateway 和 node 不在同一台机器，node 必须能通过私网/VPN 访问 gateway 的 `4000-4010`，
可以把 gateway 端口绑定到私有网卡并用防火墙限制来源。不要把 gateway 的 `4000-4010` 裸露到公网：
gateway 没有公网 allowlist，任何人访问都可能触发大流量 bootstrap replay。

这和公网 HL peers 看到的 `4001/4002` 是两件事。gateway 对下游 node 开放的端口不能替代真实 HL node
的公网 peer 行为；如果 gw 机器本身没有健康 HL node 的公网 `4001/4002` 行为，上游 peers 可能把这个源 IP
降级，表现为 peerd `header_eof`、`peer_full` 增多或 live pool 不稳定。因此生产上优先同机/同公网出口部署。

### 是否需要先停 HL node？

不需要。首次部署时反而应该先保持 HL node 正常运行，给 peerd 提供真实 peer 发现来源。
等 peerd live pool 和 gateway cache 都准备好后，再重启 node 切到 gateway。

### cache 多久刷新？

gateway 会定期刷新 bootstrap cache。当前 fresh cache 窗口是 30 分钟，刷新周期约 10 分钟；如果 live pool 临时低于 4，gateway 会先放缓刷新，接近过期时仍会尝试刷新。
切换前建议确认 `data/bootstrap.cache` 已存在并且 mtime 较新。

### node 内存高是不是 gateway 导致？

不是。HL node 和 gateway 是独立容器、独立进程。gateway 主要内存来自 bootstrap cache 和刷新过程。
node 自身同步、catch-up、状态加载也会占用大量内存，需要分别看 `docker stats`。实测双 node
同机运行时，gateway 约 4.5GB 常驻、refresh 短时更高，而单个 HL node 可到 20GB+；如果主机开始大量
使用 swap，node 可能短暂执行落后并触发 `Querying jailed validators`，这不一定是 gateway 断流。

### peerd live=0 能不能继续切？

不建议。`live=0` 表示 gateway 没有稳定上游池。已有 cache 时短时间可能还能启动，但刷新和 fallback
风险很高。应先恢复 peerd live pool。
