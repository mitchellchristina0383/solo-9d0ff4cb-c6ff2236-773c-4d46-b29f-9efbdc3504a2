"""等待采样时间线：聚合、内存上限、裁剪缺口与原始快照。

一次采样（sample）产出一条记录，记录只有三种状态：

* ``waiting`` —— 查询成功且至少观察到一条等待边；
* ``no_wait`` —— 查询成功、没有等待边；
* ``unknown`` —— 查询失败，等待状态未知（不能当成“无等待”）。

连续且“同一组等待边”的采样记录会合并成一段（segment）。等待边的身份按
**双方 PID、锁类型、锁模式、等待库位** 区分；任何一项变化都会切段。
``unknown`` 与 ``no_wait`` 是独立的状态段，且失败会把前后同签名的等待段
强制切开，不会跨失败合并。

每轮最多保留 ``max_records`` 条采样记录（默认 600）。超出时从最旧的非钉住
记录开始裁剪，被裁剪的连续区间会记录成“缺失时段”，供页面明确标注。首次
在同一次采样中看到两笔调拨互等时，该次原始快照会被钉住（保留），并计入
600 条上限。
"""

from __future__ import annotations

from typing import Iterable

MAX_WAIT_RECORDS = 600
TRANSFER_APPS = ("locklab-T1", "locklab-T2")

WAITING = "waiting"
NO_WAIT = "no_wait"
UNKNOWN = "unknown"


def _as_int(value) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return -1


def _as_text(value) -> str:
    return "" if value is None else str(value)


def edge_identity(edge: dict) -> tuple:
    """等待边的身份：双方 PID + 锁类型 + 锁模式 + 等待库位。

    相对时间、等待时长、SQL 文本等会变化的展示字段不参与身份，否则同一段
    等待会被无意义地切碎。
    """
    return (
        _as_int(edge.get("blocked_pid")),
        _as_int(edge.get("blocking_pid")),
        _as_text(edge.get("lock_type")),
        _as_text(edge.get("lock_mode")),
        _as_text(edge.get("waited_location")),
    )


def edge_identity_list(edge: dict) -> list:
    return list(edge_identity(edge))


def sample_signature(edges: Iterable[dict]) -> tuple:
    """一组等待边的稳定签名；排序后不受数据库返回顺序影响。"""
    return tuple(sorted(edge_identity(e) for e in edges))


def find_transfer_mutual_wait(edges: Iterable[dict]) -> list[dict] | None:
    """返回两笔调拨在同一次采样中互等的边对；没有则返回 None。"""
    lookup: dict[tuple[str, str], dict] = {}
    for edge in edges:
        blocked = _as_text(edge.get("blocked_app"))
        blocking = _as_text(edge.get("blocking_app"))
        if blocked in TRANSFER_APPS and blocking in TRANSFER_APPS and blocked != blocking:
            lookup[(blocked, blocking)] = edge
    for (blocked, blocking), edge in lookup.items():
        reverse = lookup.get((blocking, blocked))
        if reverse is not None:
            return [edge, reverse]
    return None


def _copy_edges(edges: Iterable[dict]) -> list[dict]:
    return [dict(e) for e in edges]


def _copy_record(record: dict | None) -> dict | None:
    if record is None:
        return None
    out = dict(record)
    out["edges"] = _copy_edges(record.get("edges") or [])
    if record.get("mutual_edges"):
        out["mutual_edges"] = _copy_edges(record["mutual_edges"])
    out["identities"] = [edge_identity_list(e) for e in out["edges"]]
    # 内部签名不直接暴露成不可读的嵌套元组
    out.pop("signature", None)
    return out


class Timeline:
    """一轮实验的等待采样时间线（非线程安全，调用方需持锁）。"""

    def __init__(self, max_records: int = MAX_WAIT_RECORDS):
        if max_records < 1:
            raise ValueError("max_records must be >= 1")
        self.max_records = int(max_records)
        self._records: list[dict] = []
        self._trimmed: list[dict] = []
        self._total_samples = 0
        self._dropped_records = 0
        self._pinned_seq: int | None = None
        self._first_t: float | None = None
        self._counts = {WAITING: 0, NO_WAIT: 0, UNKNOWN: 0}

    # ------------------------------------------------------------------ #
    # 写入
    # ------------------------------------------------------------------ #
    def append_ok(self, sample_no: int, t: float, captured_at: str,
                  edges: Iterable[dict]) -> dict:
        """记录一次成功的采样（可能没有等待边）。"""
        edges = _copy_edges(edges)
        signature = sample_signature(edges)
        record = {
            "seq": int(sample_no),
            "t": round(float(t), 3),
            "captured_at": captured_at,
            "status": WAITING if signature else NO_WAIT,
            "edges": edges,
            "error": None,
            "pinned": False,
        }
        record["signature"] = signature

        mutual = find_transfer_mutual_wait(edges)
        if mutual is not None and self._pinned_seq is None:
            # 首次观测到两笔调拨互等：钉住当次原始快照，后续裁剪不会删除它
            record["pinned"] = True
            record["pin_reason"] = "first_mutual_wait"
            record["mutual_edges"] = _copy_edges(mutual)
            self._pinned_seq = record["seq"]

        self._append(record)
        return record

    def append_error(self, sample_no: int, t: float, captured_at: str,
                     message: str) -> dict:
        """记录一次失败的采样；状态为 unknown，绝不等同于“无等待”。"""
        record = {
            "seq": int(sample_no),
            "t": round(float(t), 3),
            "captured_at": captured_at,
            "status": UNKNOWN,
            "edges": [],
            "error": _as_text(message).strip() or "等待关系查询失败",
            "pinned": False,
            "signature": None,
        }
        self._append(record)
        return record

    def _append(self, record: dict) -> None:
        self._total_samples += 1
        self._counts[record["status"]] += 1
        if self._first_t is None:
            self._first_t = record["t"]
        self._records.append(record)
        self._enforce_cap()

    def _enforce_cap(self) -> None:
        while len(self._records) > self.max_records:
            index = next(
                (i for i, r in enumerate(self._records) if not r.get("pinned")),
                None,
            )
            if index is None:  # 理论上只有一条钉住记录，防御性退出
                break
            dropped = self._records.pop(index)
            self._note_dropped(dropped)

    def _note_dropped(self, record: dict) -> None:
        self._dropped_records += 1
        last = self._trimmed[-1] if self._trimmed else None
        if last is not None and last["to_seq"] + 1 == record["seq"]:
            last["to_seq"] = record["seq"]
            last["to_t"] = record["t"]
            last["count"] += 1
        else:
            self._trimmed.append({
                "from_seq": record["seq"],
                "to_seq": record["seq"],
                "from_t": record["t"],
                "to_t": record["t"],
                "count": 1,
            })

    # ------------------------------------------------------------------ #
    # 读取 / 序列化
    # ------------------------------------------------------------------ #
    @property
    def total_samples(self) -> int:
        return self._total_samples

    @property
    def dropped_records(self) -> int:
        return self._dropped_records

    @property
    def pinned_seq(self) -> int | None:
        return self._pinned_seq

    def records(self) -> list[dict]:
        return [_copy_record(r) for r in self._records]

    def latest(self) -> dict | None:
        return _copy_record(self._records[-1]) if self._records else None

    def latest_ok(self) -> dict | None:
        for record in reversed(self._records):
            if record["status"] != UNKNOWN:
                return _copy_record(record)
        return None

    def pinned_record(self) -> dict | None:
        if self._pinned_seq is None:
            return None
        for record in self._records:
            if record["seq"] == self._pinned_seq:
                return _copy_record(record)
        return None

    def trimmed_ranges(self) -> list[dict]:
        out = []
        for rng in self._trimmed:
            item = dict(rng)
            item["message"] = (
                f"已裁剪 t={rng['from_t']}s–{rng['to_t']}s 的 "
                f"{rng['count']} 条采样（#{rng['from_seq']}–#{rng['to_seq']}），"
                "该时段原始采样缺失"
            )
            out.append(item)
        return out

    def segments(self) -> list[dict]:
        """把连续且同签名的记录合并成等待段 / 无等待段 / 未知段。

        合并要求 ``seq`` 连续：中间被裁剪掉的记录会让相邻保留记录不连续，
        因此也不会跨越未知时段或缺失时段错误合并。
        """
        segments: list[dict] = []
        for record in self._records:
            if record["status"] == WAITING:
                key = (WAITING, record["signature"])
            elif record["status"] == NO_WAIT:
                key = (NO_WAIT,)
            else:
                key = (UNKNOWN,)

            prev = segments[-1] if segments else None
            if (
                prev is not None
                and prev["_key"] == key
                and prev["last_sample"] + 1 == record["seq"]
            ):
                self._extend_segment(prev, record)
            else:
                segments.append(self._new_segment(key, record))

        return [self._public_segment(seg) for seg in segments]

    def _new_segment(self, key: tuple, record: dict) -> dict:
        return {
            "_key": key,
            "kind": record["status"],
            "first_sample": record["seq"],
            "last_sample": record["seq"],
            "first_t": record["t"],
            "last_t": record["t"],
            "first_at": record["captured_at"],
            "last_at": record["captured_at"],
            "sample_count": 1,
            "edges": _copy_edges(record["edges"]),
            "error": record["error"],
            "pinned": bool(record.get("pinned")),
            "pinned_sample": record["seq"] if record.get("pinned") else None,
        }

    def _extend_segment(self, segment: dict, record: dict) -> None:
        segment["last_sample"] = record["seq"]
        segment["last_t"] = record["t"]
        segment["last_at"] = record["captured_at"]
        segment["sample_count"] += 1
        # 段内选最新一条的边用于展示；身份字段与之前保持一致
        segment["edges"] = _copy_edges(record["edges"])
        segment["error"] = record["error"]
        if record.get("pinned"):
            segment["pinned"] = True
            segment["pinned_sample"] = record["seq"]

    def _public_segment(self, segment: dict) -> dict:
        out = {k: v for k, v in segment.items() if k != "_key"}
        out["duration_s"] = round(segment["last_t"] - segment["first_t"], 3)
        out["identities"] = [edge_identity_list(e) for e in out["edges"]]
        return out

    def items(self) -> list[dict]:
        """按采样序号把“等待段”和“裁剪缺口”归并成一条时间线。

        钉住的记录可能长期保留，使裁剪区间分成不连续的多段（例如 #1–#20 与
        #40–#60 被裁、#21–#39 恰好是钉住快照所在段），因此不能假设
        “所有缺口都在所有段之后/之前”，必须按序号归并。
        """
        entries = [
            (segment["first_sample"], 0, {"item_type": "segment", **segment})
            for segment in self.segments()
        ]
        entries += [
            (gap["from_seq"], 1, {"item_type": "gap", **gap})
            for gap in self.trimmed_ranges()
        ]
        entries.sort(key=lambda entry: (entry[0], entry[1]))
        return [entry[2] for entry in entries]

    def public(self) -> dict:
        return {
            "max_records": self.max_records,
            "total_samples": self._total_samples,
            "retained_records": len(self._records),
            "dropped_records": self._dropped_records,
            "pinned_sample": self._pinned_seq,
            "first_sample_t": self._first_t,
            "status_counts": dict(self._counts),
            "trimmed_ranges": self.trimmed_ranges(),
            "segments": self.segments(),
            "items": self.items(),
            "latest": self.latest(),
            "latest_ok": self.latest_ok(),
            "pinned_raw_snapshot": self.pinned_record(),
        }
