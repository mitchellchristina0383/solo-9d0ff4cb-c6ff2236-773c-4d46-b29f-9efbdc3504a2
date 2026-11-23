# lock-lab 测试

单元测试只依赖 Python + pytest，不需要数据库：

```bash
pip install -r requirements-dev.txt
pytest -q tests/
```

覆盖点：

- `test_wait_timeline.py`：同组等待边分段合并、按双方 PID/锁类型/模式/库位切段、
  `unknown` 与 `no_wait` 分离且不跨失败合并、600 条上限与缺失时段、
  首次互等钉住原始快照并计入上限。
- `test_run_public.py`：`/api/runs/<id>` 的 `waits` / `deadlock_detail` 形状、
  只有 40P01 才判定死锁、无环快照时明示未采到、环只出现在跨采样聚合时
  `snapshot_available=false`。
