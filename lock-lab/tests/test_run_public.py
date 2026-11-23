"""Run.public() 的集成形状测试：死锁详情、等待面板字段、600 条上限。"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

from app import Run  # noqa: E402
from wait_timeline import MAX_WAIT_RECORDS  # noqa: E402


def make_run():
    return Run(
        run_id="r1",
        lock_order="opposite",
        lock_timeout_ms=15000,
        deadlock_timeout_ms=1000,
        stagger_ms=1200,
        hard_timeout_s=30.0,
    )


def mutex_edges():
    return [
        {"blocked_pid": 11, "blocking_pid": 22, "blocked_app": "locklab-T1",
         "blocking_app": "locklab-T2", "lock_type": "transactionid",
         "lock_mode": "ShareLock", "waited_location": "A-02", "wait_seconds": 0.1},
        {"blocked_pid": 22, "blocking_pid": 11, "blocked_app": "locklab-T2",
         "blocking_app": "locklab-T1", "lock_type": "transactionid",
         "lock_mode": "ShareLock", "waited_location": "A-01", "wait_seconds": 0.1},
    ]


def test_public_shape_has_wait_panel_and_deadlock_detail():
    run = make_run()
    run.record_sample(1, 0.1, mutex_edges(), None)
    run.record_sample(2, 0.2, mutex_edges(), None)
    run.db_declared_deadlock = True
    run.deadlock_victim = "T2"
    run.wait_cycle = ["locklab-T1", "locklab-T2", "locklab-T1"]
    run.wait_cycle_detail = {"source": "single_sample",
                             "raw_snapshot": run.timeline.records()[0]}
    public = run.public()
    waits = public["waits"]
    assert waits["max_records"] == MAX_WAIT_RECORDS == 600
    assert len(waits["segments"]) == 1
    assert waits["segments"][0]["sample_count"] == 2
    assert waits["pinned_sample"] == 1
    assert waits["pinned_raw_snapshot"]["edges"][0]["waited_location"] == "A-02"
    detail = public["deadlock_detail"]
    assert detail["declared"] is True and detail["sqlstate"] == "40P01"
    assert detail["snapshot_available"] is True
    assert detail["raw_snapshot"]["seq"] == 1


def test_deadlock_without_snapshot_says_so():
    run = make_run()
    run.record_sample(1, 0.1, [], None)
    run.db_declared_deadlock = True
    run.wait_cycle = ["locklab-T1", "locklab-T2", "locklab-T1"]
    run.wait_cycle_detail = {"source": "aggregate", "raw_snapshot": None}
    detail = run.public()["deadlock_detail"]
    assert detail["declared"] is True
    assert detail["snapshot_available"] is False
    assert detail["raw_snapshot"] is None
    assert "没有捕获到完整等待环的原始快照" in detail["note"]


def test_no_40p01_never_declares_deadlock():
    run = make_run()
    # 即便观测到完整互等环，没有 40P01 也不判定死锁
    run.record_sample(1, 0.1, mutex_edges(), None)
    run.wait_cycle = ["locklab-T1", "locklab-T2", "locklab-T1"]
    detail = run.public()["deadlock_detail"]
    assert detail["declared"] is False
    assert detail["sqlstate"] is None
    assert "未返回 40P01" in detail["note"]


def test_unknown_sample_is_reported_separately():
    run = make_run()
    run.record_sample(1, 0.1, [], None)
    run.record_sample(2, 0.2, None, "OperationalError: connection lost")
    waits = run.public()["waits"]
    assert [s["kind"] for s in waits["segments"]] == ["no_wait", "unknown"]
    assert waits["latest"]["status"] == "unknown"
    assert waits["latest_ok"]["status"] == "no_wait"


def test_raw_snapshot_prefers_single_sample_cycle():
    from app import derive_wait_cycle

    run = make_run()
    # 采样 1/2 各只有一条边，采样 3 同时集齐两条 → 只有采样 3 能证明环
    run.record_sample(1, 0.1, [mutex_edges()[0]], None)
    run.record_sample(2, 0.2, [mutex_edges()[1]], None)
    run.record_sample(3, 0.3, mutex_edges(), None)
    cycle, detail = derive_wait_cycle(run)
    assert cycle == ["locklab-T1", "locklab-T2", "locklab-T1"]
    assert detail["source"] == "single_sample"
    assert detail["raw_snapshot"]["seq"] == 3


def test_cycle_only_in_aggregate_has_no_raw_snapshot():
    from app import derive_wait_cycle

    run = make_run()
    run.record_sample(1, 0.1, [mutex_edges()[0]], None)
    run.record_sample(2, 0.2, [mutex_edges()[1]], None)
    cycle, detail = derive_wait_cycle(run)
    assert cycle == ["locklab-T1", "locklab-T2", "locklab-T1"]
    assert detail["source"] == "aggregate"
    assert detail["raw_snapshot"] is None


def test_no_cycle_at_all():
    from app import derive_wait_cycle

    run = make_run()
    run.record_sample(1, 0.1, [mutex_edges()[0]], None)
    cycle, detail = derive_wait_cycle(run)
    assert cycle is None
    assert detail["source"] == "none"
