# lock-lab —— 库位调拨行锁互斥实验（Flask + PostgreSQL）

复盘两笔库位调拨互相卡住的场景。两笔样例调拨在各自独立的数据库连接/事务里
对**同两条库存记录**执行 `SELECT ... FOR UPDATE`，页面按调拨单展示 PostgreSQL
中**实际观测到**的等待关系；只有数据库自己报死锁（SQLSTATE `40P01`）才判定为死锁。

## 业务场景

| 调拨单 | 方向 | 数量 |
|---|---|---|
| TR-1001（T1） | A-01 → A-02 | 10 |
| TR-1002（T2） | A-02 → A-01 | 10 |

初始两库位各 100 件。每笔事务：`BEGIN` → 按顺序对两条记录 `FOR UPDATE` 加行锁
→ 扣减/增加库存 → `COMMIT`。

页面可切换两种加锁次序：

- **相反次序（opposite）**：T1 先 A-01 后 A-02，T2 先 A-02 后 A-01
  → 互相等待，PostgreSQL 达到 `deadlock_timeout` 后检测出等待环，回滚其中一笔，另一笔提交。
- **相同次序（same）**：两笔都按 A-01 → A-02 加锁
  → 只排队、不成环，不会死锁，两笔先后提交。

## 快速启动（Docker Compose）

```bash
cd lock-lab
docker compose up --build
# 打开 http://localhost:8000
```

- `db`：`postgres:15-alpine`，首次启动自动执行 `db/init.sql` 建表建视图，
  带健康检查；`deadlock_timeout=500ms`、`log_lock_waits=on`。
- `web`：Python 3.12 + gunicorn（1 进程 8 线程，每笔调拨独占一条 psycopg2 连接）。
- 停止并清库：`docker compose down -v`。

页面上可调参数：`lock_timeout`、`deadlock_timeout`、首锁后停顿 `stagger`
（保证两笔先各持一把锁）、硬性超时秒数。

## 实现要点

### 1. 等待关系只认数据库地面真值

`db/init.sql` 中的 `v_lock_waits` 视图以 `pg_blocking_pids()` 为等待关系来源，
关联 `pg_stat_activity` 取双方应用名、语句与状态。

> 注意一个 PG15 的实际行为：行级锁等待者在 `pg_locks` 里只留下未授予的
> `transactionid / ShareLock`，**没有 tuple 条目**，因此经典的 pg_locks
> 自连接写法能看到等待边但反查不到具体库位。本项目用 `pg_blocking_pids()`
> 保证等待边不丢；库位信息优先取视图中的 tuple 反查，缺失时由应用侧
> 根据“该连接当前正在请求的库位”补注（边数据上带 `location_source` 字段区分来源）。

### 2. 并发编排与采样

- 每轮由 supervisor 线程启动 T1、T2 两个工作线程 + 一个监控线程，
  三者在首锁请求前通过 `threading.Barrier(3)` 同时起跑，保证从第一刻起就在采样。
- 监控线程每 **100ms** 查询一次 `v_lock_waits`（使用 autocommit 只读连接，
  避免长事务快照影响观测），采样结果进入本轮的内存时间线。
- 页面展示每条等待边（谁等谁、等哪个库位、锁类型、等待时长、等待中的 SQL）。
- 等待环只用于页面高亮与定位：优先在**单次采样**里找环，找到就保留该次原始快照；
  找不到时退回全轮聚合并显式说明“未采到原始快照”。
  **死锁判定唯一依据是数据库返回的 40P01**，应用不自行推断。

### 3. 等待面板：聚合、上限与缺失

实现集中在 `app/wait_timeline.py`，页面通过 `/api/runs/<id>` 的 `waits` 字段消费：

- **采样状态三分**：`waiting`（成功且有等待边）/ `no_wait`（成功且无等待边）/
  `unknown`（查询失败）。`unknown` 绝不当成 `no_wait`。
- **分段**：连续采到的同一组等待边合并成一段。等待边身份按
  **双方 PID、锁类型、锁模式、等待库位** 区分，任何一项变化即切段；
  多边时排序后比较，不受数据库返回顺序影响。每段给出首末采样时间
  （轮次内 `t`、墙钟时间、采样序号）和采样次数。
- **不跨失败合并**：`unknown` 是独立状态段，前后同签名的等待段也因状态不同
  而分开；裁剪造成的序号不连续同样会阻断合并。
- **600 条上限**：每轮内存最多保留 600 条采样记录（含钉住的原始快照）。
  超出时从最旧的非钉住记录开始裁剪，连续被裁区间记录为“缺失时段”，
  页面在分段列表中原位标注；**面板顶部始终显示最新一次成功实测**，
  若最近一次查询失败，额外提示“等待状态未知”而不回退成无等待。
- **首次互等钉住**：第一次在同一次采样中看到 T1/T2 互等时，钉住该次原始快照
  （`waits.pinned_raw_snapshot`），此后裁剪不会删除它；钉住记录仍计入 600 条上限。

### 4. 死锁详情

- 只有 worker 收到 SQLSTATE `40P01` 才置 `db_declared_deadlock`。
- `deadlock_detail.raw_snapshot` 指向**同一次采样内集齐环上所有边**的原始快照
  （含双方 PID、锁类型/模式、等待库位、等待中的 SQL、采样序号与时间）。
- 若只有跨采样的聚合环、或环快照已被裁剪，`snapshot_available=false`，
  页面明示“没有采到等待环的原始快照”，不伪造证据。

### 5. 结局与超时

- 工作线程捕获异常后按 SQLSTATE 分类：`40P01` 死锁回滚 / `55P03` lock_timeout 回滚 / 其他回滚，
  成功则提交；最终每笔调拨单显示实际结局与说明。
- **lock_timeout**：会话级 `SET lock_timeout`，超时该笔回滚（55P03），页面显示为“回滚但非死锁”。
- **硬性超时**：监控线程到点设置取消信号并 `pg_terminate_backend()` 本轮两个后端，
  连接在 worker 的 `finally` 中也会 `close()`，确保本轮连接全部释放，之后可立刻重新试跑。
- **手动取消**：`POST /api/runs/<id>/cancel`，对阻塞中的后端调用 `pg_cancel_backend()`。
- 每轮开始前把库存重置为 100/100；轮次历史保留最近 50 轮，解锁/结束后仍可回看
  当轮等待环、被回滚的一笔、另一笔的提交结果和库存实况。

### HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 实验页面 |
| POST | `/api/runs` | 发起一轮（body：`lock_order` 等参数） |
| GET | `/api/runs/<id>` | 轮次详情（`waits` 聚合面板、`deadlock_detail`、结局、库存） |
| POST | `/api/runs/<id>/cancel` | 手动取消并释放连接 |
| GET | `/api/runs` | 历史轮次列表 |
| GET | `/api/health` | 数据库连通性 |

## 实测记录（PostgreSQL 15.20）

以下为本地真实试跑结果（每 100ms 采样；应用以 Dockerfile 同款
`gunicorn --workers 1 --threads 8` 启动）：

### 相反次序 —— 数据库判定死锁

run `09a48d7e040d`（`stagger=1.2s`，`deadlock_timeout=1s`）：

- `db_declared_deadlock=true`，T2 收到 `40P01` 被回滚，T1 提交。
  （具体牺牲哪一笔由 PostgreSQL 死锁检测器选择；多轮试跑中也观察到 T1 被回滚。
  例如 `3e6c90e8fc24` 采样间隔窗口没覆盖住等待期，同样会按 40P01 判定死锁。）
- 等待面板分段（`waits.segments`）：
  - `no_wait` #1–#14，14 次采样
  - `waiting` #15–#23，9 次采样；两条边
    `T1(pid 1790) → T2(pid 1791) @ A-02`、`T2(pid 1791) → T1(pid 1790) @ A-01`，
    锁类型 `transactionid / ShareLock`；段首 `t=1.576s`、段末 `t=2.403s`
  - `no_wait` #24，1 次采样
- 第 #15 次采样是**首次互等**，被钉住：`waits.pinned_sample=15`，
  `deadlock_detail.raw_snapshot.seq=15`，`snapshot_available=true`（页面展示该次
  原始边：双方 PID、锁类型/模式、等待库位、等待中的 SQL）。
- 库存只体现 T1 的调拨：A-01=90、A-02=110（T2 回滚不留痕迹）。

### 相同次序 —— 普通等待，无死锁

- `db_declared_deadlock=false`，两笔均 `committed`，无等待环。
- 采样到单边排队（如 `locklab-T1 → locklab-T2 @ A-01`），持续约 1s 后放行。
- 两笔对冲后库存回到 100/100。

### 长等待 + 超过 600 条上限

run `aec7c1383e88`（`stagger=75s`，实际等待约 60s，共 616 次采样）：

- `waits.retained_records=600`、`total_samples=616`、`dropped_records=16`；
  `trimmed_ranges` 给出缺失时段：`t=0.11s–1.678s，#1–#16`，页面以“缺失时段”
  置顶标出，面板顶部仍显示最新一次成功实测。
- 首次互等在第 #29 次采样被钉住（`pinned_sample=29`，`deadlock_detail` 引用同一快照），
  裁剪没有删掉它，且它仍计入 600 条上限。
- 运行中把 `v_lock_waits` 临时改名制造真实查询失败：连续 360 次采样进入
  `unknown` 段（#257–#616，错误原文 `relation "v_lock_waits" does not exist`），
  与前后的 `waiting` 段/`no_wait` 段分别独立，没有被合并成一段。
- 恢复视图后立刻发起新一轮 `f47d360524fe`：分段为
  `no_wait #1–#28 → waiting #29–#91 → unknown #92–#263 → waiting #264–#273 → no_wait #274`。
  失败段前后的两个 `waiting` 段签名相同但**没有合并**，符合预期；
  该轮最终仍以 `40P01` 判死锁，且等待环原始快照可定位（#29）。

### 死锁但没采到环快照

- `stagger=1.2s`、`deadlock_timeout=50ms` 连跑 5 轮（如 `3e6c90e8fc24`）：
  数据库均返回 `40P01`，但等待窗口短于采样间隔，`waits.segments` 只有 `no_wait`。
- 此时 `deadlock_detail.declared=true`、`snapshot_available=false`、
  `raw_snapshot=null`，页面死锁横幅与详情区都明示
  “没有采到等待环的原始快照”，不画环、不伪造。

### 异常路径

- **lock_timeout 路径**：`lock_timeout=800ms` 且把 `deadlock_timeout` 调到 30s，
  一笔收到 `55P03` 回滚、另一笔提交，页面明确显示“回滚但数据库未判定为死锁”，
  库存只体现提交那一笔。
- **硬性超时**：`hard_timeout_s=5` 到点后轮次置为 `cancelled`，
  `pg_terminate_backend` 生效，`pg_stat_activity` 中 `locklab-*` 连接数为 0，
  随后可立即发起新一轮并正常完成。
- **手动取消**：取消接口返回后两笔回滚、连接数归 0。

## 测试

```bash
pip install -r tests/requirements-dev.txt
pytest -q tests/
```

`tests/test_wait_timeline.py` 覆盖分段/身份切段/未知与无等待分离/600 条上限与
缺失时段/钉住与中间缺口；`tests/test_run_public.py` 覆盖 API 形状、
只认 40P01、环快照可否定位。当前 20 项全部通过。
另用 jsdom 对真实模板做过渲染冒烟（伪造 `/api` 响应，验证缺失时段、
未知段、最新实测回退与原始快照位），并用 headless Chromium 对上述真实轮次截图核对。

## 目录结构

```
lock-lab/
├── docker-compose.yml        # db(healthcheck) + web
├── README.md
├── db/
│   └── init.sql              # inventory 表 + v_lock_waits 观测视图
├── tests/
│   ├── test_wait_timeline.py # 聚合/上限/裁剪/失败分段
│   ├── test_run_public.py    # API 形状与死锁详情
│   └── requirements-dev.txt
└── app/
    ├── Dockerfile
    ├── requirements.txt
    ├── app.py                # 编排、工作线程、监控、超时、HTTP API
    ├── wait_snapshot.py      # 视图只读查询
    ├── wait_timeline.py      # 采样聚合、600 条上限、缺失时段、原始快照
    └── templates/index.html  # 单页前端
```

## 余项 / 已知边界

1. **交付环境未安装 Docker**（无 docker CLI/daemon、无 `CAP_SYS_ADMIN`、
   user namespace 亦被禁用），因此 `docker compose up` 未在本机实际执行。
   已完成的替代校验：
   - `docker-compose config`（v1.29.2，离线解析）成功，展开后的 `db`/`web`
     服务、卷挂载与 `depends_on: service_healthy` 均符合预期；
   - compose-spec 官方 JSON Schema 校验通过；
   - 按 Dockerfile 的 `gunicorn --workers 1 --threads 8` 命令在本机
     原生 PostgreSQL 15.20 上跑通全部实测路径（容器内逻辑一致）。
   仍需在有 Docker 的机器上执行一次 `docker compose up --build` 做最终确认。
2. 等待关系采样为 100ms 周期快照，极短于 100ms 的等待窗口可能采不到；
   默认 `deadlock_timeout=1000ms` 正是为了让等待环稳定落入采样窗口。
   采不到时页面按“未采到原始快照”明示，不会自造环。
3. 为便于复盘，全局同一时间只允许一轮实验在跑（第二个请求返回 409）；
   历史轮次保存在进程内存（最近 50 轮，每轮最多 600 条采样），重启进程后清空，未做持久化。
4. 库存表仅两条样例记录、调拨数量写死，聚焦锁等待演示，未做通用库存建模。
5. 前端轮询间隔 200ms，慢于 100ms 采样，因此面板展示的是聚合段与最新实测，
   不保证逐条动画；原始证据以 `/api/runs/<id>` 的 `waits` 为准。
