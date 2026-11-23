"""等待采样聚合 / 上限 / 裁剪 / 失败分段的单元测试。"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

from wait_timeline import (  # noqa: E402
    MAX_WAIT_RECORDS,
    NO_WAIT,
    UNKNOWN,
    WAITING,
    Timeline,
    find_transfer_mutual_wait,
)


def edge(blocked_pid=101, blocking_pid=102, blocked="locklab-T1",
         blocking="locklab-T2", lock_type="transactionid",
         lock_mode="ShareLock", loc="A-02", wait_seconds=0.1):
    return {
        "blocked_pid": blocked_pid,
        "blocking_pid": blocking_pid,
        "blocked_app": blocked,
        "blocking_app": blocking,
        "lock_type": lock_type,
        "lock_mode": lock_mode,
        "waited_location": loc,
        "wait_seconds": wait_seconds,
        "blocked_query": "SELECT ... FOR UPDATE",
    }


def build(samples):
    """samples 为 [(edges 或 Exception)]，按 0.1s 递进。"""
    tl = Timeline()
    for i, item in enumerate(samples, start=1):
        t = round(i * 0.1, 3)
        if isinstance(item, Exception):
            message = f"{type(item).__name__}: {item}"
            tl.append_error(i, t, f"2026-01-01T00:00:{i:02d}.000Z", message)
        else:
            tl.append_ok(i, t, f"2026-01-01T00:00:{i:02d}.000Z", item)
    return tl


def test_consecutive_same_edges_merge_into_one_segment():
    tl = build([[edge(wait_seconds=0.1)], [edge(wait_seconds=0.2)], [edge(wait_seconds=0.3)]])
    segs = tl.segments()
    assert len(segs) == 1
    assert segs[0]["kind"] == WAITING
    assert segs[0]["sample_count"] == 3
    assert segs[0]["first_sample"] == 1 and segs[0]["last_sample"] == 3
    assert segs[0]["first_t"] == 0.1 and segs[0]["last_t"] == 0.3
    assert segs[0]["duration_s"] == pytest.approx(0.2)


def test_identity_change_splits_segment():
    base = [edge()]
    changed_pid = [edge(blocked_pid=999)]
    changed_mode = [edge(lock_mode="ExclusiveLock")]
    changed_loc = [edge(loc="A-01")]
    changed_type = [edge(lock_type="tuple")]
    tl = build([base, base, changed_pid, changed_mode, changed_loc, changed_type])
    segs = tl.segments()
    assert len(segs) == 5
    assert [s["sample_count"] for s in segs] == [2, 1, 1, 1, 1]


def test_multi_edge_group_order_independent():
    a = edge(blocked_pid=101, blocking_pid=102, blocked="locklab-T1",
             blocking="locklab-T2", loc="A-02")
    b = edge(blocked_pid=102, blocking_pid=101, blocked="locklab-T2",
             blocking="locklab-T1", loc="A-01")
    tl = build([[a, b], [b, a]])
    segs = tl.segments()
    assert len(segs) == 1
    assert segs[0]["sample_count"] == 2


def test_unknown_is_not_no_wait_and_breaks_merge():
    tl = build([[edge()], [edge()], RuntimeError("query failed"), [edge()], [edge()]])
    segs = tl.segments()
    assert [s["kind"] for s in segs] == [WAITING, UNKNOWN, WAITING]
    assert [s["sample_count"] for s in segs] == [2, 1, 2]
    assert segs[1]["error"].startswith("RuntimeError")
    assert segs[1]["edges"] == []
    counts = tl.public()["status_counts"]
    assert counts == {WAITING: 4, UNKNOWN: 1, NO_WAIT: 0}


def test_no_wait_and_unknown_are_separate_segments():
    tl = build([[], [], RuntimeError("down"), [], RuntimeError("down"), []])
    segs = tl.segments()
    assert [s["kind"] for s in segs] == [NO_WAIT, UNKNOWN, NO_WAIT, UNKNOWN, NO_WAIT]


def test_latest_and_latest_ok_with_trailing_failure():
    tl = build([[edge()], [edge()], RuntimeError("down")])
    public = tl.public()
    assert public["latest"]["status"] == UNKNOWN
    assert public["latest_ok"]["status"] == WAITING
    assert public["latest_ok"]["seq"] == 2


def test_cap_is_600_and_trimmed_ranges_reported():
    tl = Timeline(MAX_WAIT_RECORDS)
    for i in range(1, 706):
        tl.append_ok(i, i * 0.1, "2026-01-01T00:00:00.000Z", [edge()])
    public = tl.public()
    assert public["retained_records"] == 600
    assert public["max_records"] == 600
    assert public["dropped_records"] == 105
    assert public["trimmed_ranges"][0]["from_seq"] == 1
    assert public["trimmed_ranges"][0]["to_seq"] == 105
    # 最新实测始终可见
    assert public["latest"]["seq"] == 705
    assert public["latest_ok"]["seq"] == 705
    # 缺失时段出现在分段列表最前面
    assert public["items"][0]["item_type"] == "gap"
    assert "缺失" in public["items"][0]["message"]


def test_first_mutual_wait_pins_raw_snapshot_even_under_cap():
    a = edge(blocked_pid=101, blocking_pid=102, blocked="locklab-T1",
             blocking="locklab-T2", loc="A-02")
    b = edge(blocked_pid=102, blocking_pid=101, blocked="locklab-T2",
             blocking="locklab-T1", loc="A-01")
    tl = Timeline(10)
    tl.append_ok(1, 0.1, "t1", [a, b])
    for i in range(2, 60):
        tl.append_ok(i, i * 0.1, "t", [edge()])
    public = tl.public()
    assert public["pinned_sample"] == 1
    assert public["pinned_raw_snapshot"]["seq"] == 1
    assert len(public["pinned_raw_snapshot"]["edges"]) == 2
    assert public["retained_records"] == 10
    assert 1 not in [r["seq"] for r in public["segments"] if r["kind"] == "gap"]
    # 钉住记录也计入上限：总保留不超过 10
    assert public["retained_records"] <= 10


def test_first_mutual_wait_only_pins_first_occurrence():
    a = edge(blocked_pid=101, blocking_pid=102, blocked="locklab-T1",
             blocking="locklab-T2", loc="A-02")
    b = edge(blocked_pid=102, blocking_pid=101, blocked="locklab-T2",
             blocking="locklab-T1", loc="A-01")
    tl = build([[a, b], [a, b]])
    assert tl.pinned_seq == 1


def test_single_direction_does_not_pin():
    tl = build([[edge()]])
    assert tl.pinned_seq is None
    assert tl.public()["pinned_raw_snapshot"] is None


def test_find_transfer_mutual_wait_requires_both_transfers():
    a = edge(blocked="locklab-T1", blocking="locklab-T2")
    only = [a]
    assert find_transfer_mutual_wait(only) is None
    b = edge(blocked="locklab-T2", blocking="locklab-T1")
    got = find_transfer_mutual_wait([a, b])
    assert got is not None and len(got) == 2


def test_segments_do_not_merge_across_trimmed_gap():
    tl = Timeline(4)
    for i in range(1, 8):
        tl.append_ok(i, i * 0.1, "t", [edge()])
    # 保留 4..7；1..3 被裁剪，不能把 1..7 合并成一段
    public = tl.public()
    assert len(public["segments"]) == 1
    assert public["segments"][0]["first_sample"] == 4
    assert public["segments"][0]["last_sample"] == 7
    assert public["items"][0]["from_seq"] == 1


def test_items_keep_pinned_middle_segment_between_two_gaps():
    a = edge(blocked_pid=101, blocking_pid=102, blocked="locklab-T1",
             blocking="locklab-T2", loc="A-02")
    b = edge(blocked_pid=102, blocking_pid=101, blocked="locklab-T2",
             blocking="locklab-T1", loc="A-01")
    tl = Timeline(6)
    # 第 5 次采样是首次互等（钉住）；继续采样后，前后都会出现裁剪缺口
    for i in range(1, 5):
        tl.append_ok(i, i * 0.1, "t", [edge()])
    tl.append_ok(5, 0.5, "t", [a, b])
    for i in range(6, 20):
        tl.append_ok(i, i * 0.1, "t", [edge()])
    public = tl.public()
    assert public["retained_records"] == 6
    assert public["pinned_sample"] == 5
    kinds = [(item["item_type"], item.get("from_seq") or item.get("first_sample"))
             for item in public["items"]]
    # 缺口 #1–#4 → 钉住段 #5 → 缺口 #6–#14 → 后续段：严格按序号递增
    seqs = [s for _, s in kinds]
    assert seqs == sorted(seqs)
    assert [k for k, _ in kinds] == ["gap", "segment", "gap", "segment"]
    assert (kinds[1][1]) == 5
    assert public["trimmed_ranges"][0]["to_seq"] == 4
    assert public["trimmed_ranges"][1]["from_seq"] == 6
