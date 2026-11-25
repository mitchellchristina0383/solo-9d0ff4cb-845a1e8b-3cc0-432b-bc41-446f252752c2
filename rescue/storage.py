"""持久化层：样盘保存、扇区原子写入、崩溃对账、同进度快照。

设计要点
--------
- 镜像按 512 字节分扇区，最多 8 MiB（MAX_IMAGE_SIZE）。
- 每个已救出的扇区写成独立文件 sectors/XX/<index>.bin。
  写入走 "临时文件 + fsync + rename + 目录 fsync"，保证单扇区要么完整
  落盘、要么完全不存在；写到一半的临时文件在启动对账时删除，仍算缺失。
- job.json 是状态缓存，磁盘上的扇区文件才是真相；重启时以文件系统
  为准重建状态，因此中断重启后已完整保存的扇区仍然可用。
- 快照在生成瞬间把镜像原字节与元数据落到 snapshots/<id>/，之后读盘、
  核对退回、进程重启都不会改变已生成的快照。
- 用样盘核对时只退回「当前字节仍等于本轮发现错误内容」的已救扇区，
  退回即删文件并持久化，重启后仍算缺失；扫描期间不持有存储锁。
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import uuid
from typing import Dict, Iterable, List, Optional, Set, Tuple

SECTOR_SIZE = 512
MAX_IMAGE_SIZE = 8 * 1024 * 1024  # 8 MiB

JOB_FILE = "job.json"
SAMPLE_FILE = "sample.img"
SECTORS_DIR = "sectors"
SNAPSHOTS_DIR = "snapshots"
TMP_SUFFIX = ".tmp"
_TMP_RE = re.compile(r"^(\d+)\.tmp$")
_SECTOR_RE = re.compile(r"^(\d+)\.bin$")
_SNAPSHOT_ID_RE = re.compile(r"^s(\d{6})$")


class StorageError(Exception):
    """请求无效（镜像不合规、扇区号越界等）。"""


class JobExistsError(StorageError):
    """已有一轮救援任务，拒绝再次上传。"""


class SampleUnreadableError(StorageError):
    """样盘缺失、不完整或与建任务时不一致，本轮核对不产生任何改动。"""


class JobChangedError(StorageError):
    """核对期间任务被重置或替换，本轮核对结果作废。"""


class VerificationBusyError(StorageError):
    """已有一轮核对在进行，拒绝并发核对。"""


def _shard_dir(sectors_root: str, index: int) -> str:
    return os.path.join(sectors_root, f"{index % 1000:03d}")


def _sector_path(sectors_root: str, index: int) -> str:
    return os.path.join(_shard_dir(sectors_root, index), f"{index}.bin")


class JobStorage:
    """一个工作目录对应一份救援任务，所有方法线程安全。"""

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.job_path = os.path.join(data_dir, JOB_FILE)
        self.sample_path = os.path.join(data_dir, SAMPLE_FILE)
        self.sectors_root = os.path.join(data_dir, SECTORS_DIR)
        self.snapshots_root = os.path.join(data_dir, SNAPSHOTS_DIR)
        self._lock = threading.RLock()
        self._verify_gate = threading.Lock()
        self._snapshots: Dict[str, dict] = {}
        self._snapshot_counter = 0
        self._state: dict = self._load_or_init()
        self._load_snapshots()

    # ------------------------------------------------------------------ #
    # 启动与状态
    # ------------------------------------------------------------------ #
    def _load_or_init(self) -> dict:
        os.makedirs(self.sectors_root, exist_ok=True)
        # 崩溃对账：删除任何写到一半的临时扇区文件与状态临时文件。
        for name in os.listdir(self.data_dir):
            if name.startswith(JOB_FILE) and name.endswith(TMP_SUFFIX):
                _unlink_quiet(os.path.join(self.data_dir, name))
        for shard in os.listdir(self.sectors_root):
            shard_path = os.path.join(self.sectors_root, shard)
            if not os.path.isdir(shard_path):
                continue
            for name in os.listdir(shard_path):
                if _TMP_RE.match(name):
                    try:
                        os.unlink(os.path.join(shard_path, name))
                    except FileNotFoundError:
                        pass

        saved = self._scan_saved_sectors()

        if os.path.exists(self.job_path):
            with open(self.job_path, "r", encoding="utf-8") as fh:
                state = json.load(fh)
            # 磁盘真相优先：状态里声称已救、但文件不在了的扇区剔除。
            # 内部用集合维护，避免每次写入都对不断增长的列表排序（O(n^2)）。
            state["saved"] = {i for i in saved if i < state["sector_count"]}
            # 旧版本任务没有 job_id，补一个并落盘，供核对判断任务是否被换过。
            if not state.get("job_id"):
                state["job_id"] = uuid.uuid4().hex
        else:
            state = None

        if state is None:
            return {
                "job_id": None,
                "sector_count": 0,
                "saved": set(),
                "sample_sha256": None,
                "sample_size": 0,
                "created_at": None,
            }

        self._persist_state(state)
        return state

    def _scan_saved_sectors(self) -> Set[int]:
        saved: Set[int] = set()
        if not os.path.isdir(self.sectors_root):
            return saved
        for shard in os.listdir(self.sectors_root):
            shard_path = os.path.join(self.sectors_root, shard)
            if not os.path.isdir(shard_path):
                continue
            for name in os.listdir(shard_path):
                match = _SECTOR_RE.match(name)
                if match:
                    saved.add(int(match.group(1)))
        return saved

    def _persist_state(self, state: Optional[dict] = None) -> None:
        state = state or self._state
        # 临时名带线程标识，避免并发持久化时共用同一 tmp 路径。
        tmp_path = f"{self.job_path}.{os.getpid()}.{threading.get_ident()}.{TMP_SUFFIX}"
        payload = dict(state)
        payload["saved"] = sorted(state["saved"])  # 落盘时才排序
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, self.job_path)
        # fsync 目录，保证 rename 本身落盘。
        dir_fd = os.open(self.data_dir, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    def has_job(self) -> bool:
        with self._lock:
            return self._state["sector_count"] > 0

    def get_state(self) -> dict:
        with self._lock:
            return self._public_state(self._state)

    def _public_state(self, state: dict) -> dict:
        count = state["sector_count"]
        saved = sorted(state["saved"])
        saved_set = set(saved)
        missing = [i for i in range(count) if i not in saved_set]
        return {
            "job_id": state.get("job_id"),
            "sector_count": count,
            "saved_count": len(saved),
            "missing_count": len(missing),
            "saved": saved,
            "missing": missing,
            "sample_size": state["sample_size"],
            "sample_sha256": state["sample_sha256"],
            "created_at": state["created_at"],
            "complete": count > 0 and not missing,
        }

    def missing_sectors(self) -> List[int]:
        with self._lock:
            saved = self._state["saved"]
            return [i for i in range(self._state["sector_count"]) if i not in saved]

    # ------------------------------------------------------------------ #
    # 建任务：流式保存样盘
    # ------------------------------------------------------------------ #
    def create_job(self, stream) -> dict:
        """从上传流读取样盘并建任务。

        - 必须是 512 字节的整数倍，且 1 扇区 <= 大小 <= 8 MiB。
        - 流式落盘并实时校验大小，超限直接拒绝，避免把大文件读进内存。
        """
        with self._lock:
            if self.has_job():
                raise JobExistsError("已存在救援任务，请先重置后再上传新样盘")

            os.makedirs(self.sectors_root, exist_ok=True)
            tmp_sample = self.sample_path + TMP_SUFFIX
            hasher = hashlib.sha256()
            total = 0
            try:
                with open(tmp_sample, "wb") as fh:
                    while True:
                        chunk = stream.read(64 * 1024)
                        if not chunk:
                            break
                        total += len(chunk)
                        if total > MAX_IMAGE_SIZE:
                            raise StorageError(
                                f"样盘超过上限 {MAX_IMAGE_SIZE} 字节（8 MiB）"
                            )
                        hasher.update(chunk)
                        fh.write(chunk)
                    fh.flush()
                    os.fsync(fh.fileno())
            except StorageError:
                _unlink_quiet(tmp_sample)
                raise
            except Exception:
                _unlink_quiet(tmp_sample)
                raise

            if total == 0:
                _unlink_quiet(tmp_sample)
                raise StorageError("上传内容为空")
            if total % SECTOR_SIZE != 0:
                _unlink_quiet(tmp_sample)
                raise StorageError(
                    f"样盘大小 {total} 字节不是 {SECTOR_SIZE} 字节的整数倍"
                )

            os.replace(tmp_sample, self.sample_path)
            sector_count = total // SECTOR_SIZE
            state = {
                "job_id": uuid.uuid4().hex,
                "sector_count": sector_count,
                "saved": set(),
                "sample_sha256": hasher.hexdigest(),
                "sample_size": total,
                "created_at": _utc_now(),
            }
            self._state = state
            self._snapshots.clear()
            self._snapshot_counter = 0
            self._clear_snapshot_files()
            self._persist_state(state)
            return self._public_state(state)

    # ------------------------------------------------------------------ #
    # 读盘：逐个扇区原子写入
    # ------------------------------------------------------------------ #
    def save_sector(self, index: int, data: bytes, persist: bool = True) -> bool:
        """保存一个已读出的扇区。

        返回 True 表示本轮新写入，False 表示该扇区此前已完整保存。
        全零扇区只要真的读到了就正常保存，不算缺失。

        persist=False 时只更新内存状态、不写 job.json，供批量接口在
        整批结束时统一落一次状态；崩溃也不影响正确性——重启时以磁盘上
        的扇区文件为准重建状态。
        """
        with self._lock:
            count = self._state["sector_count"]
            if not self._valid_index(index, count):
                raise StorageError(f"扇区号越界：{index}，合法范围 0..{count - 1}")
            if len(data) != SECTOR_SIZE:
                raise StorageError(
                    f"扇区数据长度必须为 {SECTOR_SIZE} 字节，实际 {len(data)}"
                )

            target = _sector_path(self.sectors_root, index)
            if os.path.exists(target):
                return False

            os.makedirs(os.path.dirname(target), exist_ok=True)
            tmp = target + TMP_SUFFIX
            with open(tmp, "wb") as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, target)
            # fsync 分片目录，保证 rename 落盘。
            dir_fd = os.open(os.path.dirname(target), os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)

            self._state["saved"].add(index)
            if persist:
                self._persist_state()
            return True

    def save_batch(self, items: Iterable[Tuple[int, bytes]]) -> dict:
        """批量保存，扇区逐个原子落盘，整批只持久化一次状态。

        整批在同一把锁内完成，避免并发批量请求交错持久化状态。
        """
        saved_now: List[int] = []
        already: List[int] = []
        rejected: List[dict] = []
        with self._lock:
            for index, data in items:
                try:
                    is_new = self.save_sector(index, data, persist=False)
                except StorageError as exc:
                    rejected.append({"sector": index, "error": str(exc)})
                    continue
                (saved_now if is_new else already).append(index)
            if saved_now:
                self._persist_state()
        return {
            "saved_now": sorted(saved_now),
            "already_saved": sorted(already),
            "rejected": rejected,
            "progress": self.get_state(),
        }

    @staticmethod
    def _valid_index(index: int, count: int) -> bool:
        return isinstance(index, int) and 0 <= index < count

    def read_saved_sector(self, index: int) -> Optional[bytes]:
        with self._lock:
            count = self._state["sector_count"]
            if not self._valid_index(index, count):
                raise StorageError(f"扇区号越界：{index}")
            path = _sector_path(self.sectors_root, index)
            if not os.path.exists(path):
                return None
            with open(path, "rb") as fh:
                return fh.read()

    # ------------------------------------------------------------------ #
    # 核对样盘：找出「读足 512 字节但内容读错」的扇区并退回缺失
    # ------------------------------------------------------------------ #
    def _sample_usable(self, expected_size: int, expected_sha256: str) -> None:
        """确认样盘完整可读且与建任务时一致，否则抛出 SampleUnreadableError。"""
        try:
            stat = os.stat(self.sample_path)
        except OSError as exc:
            raise SampleUnreadableError(f"样盘不可读：{exc}") from exc
        if stat.st_size != expected_size:
            raise SampleUnreadableError(
                f"样盘不完整：期望 {expected_size} 字节，实际 {stat.st_size} 字节"
            )
        hasher = hashlib.sha256()
        total = 0
        try:
            with open(self.sample_path, "rb") as fh:
                while True:
                    chunk = fh.read(64 * 1024)
                    if not chunk:
                        break
                    total += len(chunk)
                    hasher.update(chunk)
        except OSError as exc:
            raise SampleUnreadableError(f"样盘读取失败：{exc}") from exc
        if total != expected_size or hasher.hexdigest() != expected_sha256:
            raise SampleUnreadableError("样盘内容与建任务时不一致，拒绝据此核对")

    def _scan_saved_vs_sample(self, start_saved: List[int]) -> List[Tuple[int, bytes]]:
        """逐扇区比较已救文件与样盘，返回 (扇区号, 当时错误字节) 列表。

        不持有存储锁，补读可以同时进行。已救文件只做整文件读取：
        扇区写入是原子 rename，读到的要么完整、要么文件不存在。
        """
        unreadable: List[int] = []
        suspects: List[Tuple[int, bytes]] = []
        try:
            with open(self.sample_path, "rb") as sample_fh:
                for index in start_saved:
                    sample_fh.seek(index * SECTOR_SIZE)
                    expected = sample_fh.read(SECTOR_SIZE)
                    if len(expected) != SECTOR_SIZE:
                        raise SampleUnreadableError(
                            f"样盘读取不完整：扇区 {index} 只读到 {len(expected)} 字节"
                        )
                    path = _sector_path(self.sectors_root, index)
                    try:
                        with open(path, "rb") as fh:
                            current = fh.read()
                    except FileNotFoundError:
                        continue  # 已不在磁盘上，本就缺失
                    except OSError:
                        unreadable.append(index)
                        continue
                    if current != expected:
                        suspects.append((index, current))
        except OSError as exc:
            raise SampleUnreadableError(f"样盘读取失败：{exc}") from exc
        if unreadable:
            # 有已救扇区当前读不出来，无法判断错误内容是否还在；本轮作废。
            raise StorageError(
                "以下已救扇区当前无法读取，本轮核对作废（进度未改动）："
                + ",".join(str(i) for i in unreadable)
            )
        return suspects

    def verify_saved_sectors(self) -> dict:
        """用样盘核对核对开始时已救出的扇区。

        - 扫描期间不持有存储锁，补读请求照常处理。
        - 只记录与样盘不一致的扇区，以及当时读到的错误字节。
        - 提交阶段再次确认任务未被重置/替换、样盘仍可读，然后逐个复核
          扇区文件的当前字节：
            * 仍等于本轮发现的错误内容 -> 退回缺失（删除文件并持久化）；
            * 文件已不存在 -> 本就缺失，跳过；
            * 当前字节已不是错误内容 -> 保留，不退回。
        - 样盘读不全、任务中途被换掉：整轮作废，不做任何改动。
        """
        if not self._verify_gate.acquire(blocking=False):
            raise VerificationBusyError("已有一轮核对正在进行")

        try:
            with self._lock:
                if self._state["sector_count"] == 0:
                    raise StorageError("尚无救援任务")
                job_id = self._state.get("job_id")
                sample_size = self._state["sample_size"]
                sample_sha256 = self._state["sample_sha256"]
                # 「核对开始时已救出的扇区」：只核对这个起点集合。
                start_saved = sorted(self._state["saved"])

            # 开扫前确认样盘完整；不持锁，读盘可继续。
            self._sample_usable(sample_size, sample_sha256)
            with self._lock:
                self._ensure_same_job(job_id, sample_size, sample_sha256)
            suspects = self._scan_saved_vs_sample(start_saved)

            with self._lock:
                self._ensure_same_job(job_id, sample_size, sample_sha256)

                # 第一段：只读复核，确定哪些扇区当前字节仍是本轮发现的错误
                # 内容。此段若出错，尚未改动任何文件。
                to_retract: List[int] = []
                kept_changed: List[int] = []
                for index, bad_bytes in suspects:
                    path = _sector_path(self.sectors_root, index)
                    try:
                        with open(path, "rb") as fh:
                            current = fh.read()
                    except FileNotFoundError:
                        continue  # 已被退回或本就缺失
                    except OSError:
                        raise StorageError(
                            f"复核扇区 {index} 时读取失败，本轮核对未提交任何改动"
                        )
                    if current == bad_bytes:
                        to_retract.append(index)
                    else:
                        # 当前字节已不是本轮发现的错误内容，保留。
                        kept_changed.append(index)

                # 第二段：删除退回的扇区文件并持久化状态。
                retracted: List[int] = []
                failed: List[int] = []
                for index in to_retract:
                    path = _sector_path(self.sectors_root, index)
                    try:
                        os.unlink(path)
                    except FileNotFoundError:
                        pass
                    except OSError:
                        failed.append(index)
                        continue
                    _fsync_dir(os.path.dirname(path))
                    self._state["saved"].discard(index)
                    retracted.append(index)

                if retracted:
                    self._persist_state()
                progress = self._public_state(self._state)
                if failed:
                    raise StorageError(
                        "以下扇区退回时删除文件失败，仍保留为已救（其余已退回）："
                        + ",".join(str(i) for i in failed)
                    )

            return {
                "checked": len(start_saved),
                "mismatched": sorted(i for i, _ in suspects),
                "retracted": sorted(retracted),
                "kept_changed": sorted(kept_changed),
                "missing_count": progress["missing_count"],
                "progress": progress,
            }
        finally:
            self._verify_gate.release()

    def _ensure_same_job(
        self, job_id: Optional[str], sample_size: int, sample_sha256: str
    ) -> None:
        """提交前确认任务仍是核对开始时的那个；换过任务则整轮作废。"""
        if self._state["sector_count"] == 0 or self._state.get("job_id") != job_id:
            raise JobChangedError("核对期间任务已重置或被替换，本轮结果作废")
        if (
            self._state["sample_size"] != sample_size
            or self._state["sample_sha256"] != sample_sha256
        ):
            raise JobChangedError("核对期间任务样盘已变更，本轮结果作废")

    # ------------------------------------------------------------------ #
    # 同进度快照：镜像 + 缺失清单
    # ------------------------------------------------------------------ #
    def create_snapshot(self) -> dict:
        """生成一份不可变进度快照，镜像与缺失清单共用同一 snapshot_id。

        快照内容（镜像原字节 + 缺失清单）在生成瞬间落盘到
        snapshots/<id>/，此后无论补读、核对退回还是进程重启都不再变化。
        """
        with self._lock:
            count = self._state["sector_count"]
            if count == 0:
                raise StorageError("尚无救援任务")
            self._snapshot_counter += 1
            snapshot_id = f"s{self._snapshot_counter:06d}"
            saved = sorted(self._state["saved"])
            saved_set = set(saved)
            missing = [i for i in range(count) if i not in saved_set]
            created_at = _utc_now()
            snapshot = {
                "snapshot_id": snapshot_id,
                "sector_count": count,
                "sector_size": SECTOR_SIZE,
                "saved": saved,
                "missing": missing,
                "sample_sha256": self._state["sample_sha256"],
                "created_at": created_at,
            }
            self._write_snapshot_files(snapshot)
            self._snapshots[snapshot_id] = snapshot
            return dict(snapshot)

    def _write_snapshot_files(self, snapshot: dict) -> None:
        snapshot_id = snapshot["snapshot_id"]
        target_dir = os.path.join(self.snapshots_root, snapshot_id)
        tmp_dir = target_dir + TMP_SUFFIX
        _remove_tree(tmp_dir)
        try:
            os.makedirs(tmp_dir)
            zero = b"\x00" * SECTOR_SIZE
            saved_set = set(snapshot["saved"])
            image_tmp = os.path.join(tmp_dir, "image.bin")
            with open(image_tmp, "wb") as fh:
                for index in range(snapshot["sector_count"]):
                    if index in saved_set:
                        path = _sector_path(self.sectors_root, index)
                        with open(path, "rb") as src:
                            data = src.read()
                        if len(data) < SECTOR_SIZE:
                            data = data + zero[len(data):]
                        fh.write(data[:SECTOR_SIZE])
                    else:
                        fh.write(zero)
                fh.flush()
                os.fsync(fh.fileno())
            meta_tmp = os.path.join(tmp_dir, "meta.json")
            with open(meta_tmp, "w", encoding="utf-8") as fh:
                json.dump(snapshot, fh, ensure_ascii=False, indent=2)
                fh.flush()
                os.fsync(fh.fileno())
            _fsync_dir(tmp_dir)
            _remove_tree(target_dir)
            os.replace(tmp_dir, target_dir)
            _fsync_dir(self.snapshots_root)
        except BaseException:
            _remove_tree(tmp_dir)
            raise

    def open_snapshot_image(self, snapshot_id: str):
        """流式产出快照生成瞬间落盘的救援镜像；缺失位置已补零。

        返回前先打开文件（在锁内），之后即使任务被重置或新任务开始，
        已打开的句柄仍能读完这份快照；响应关闭时迭代器会关掉句柄。
        """
        with self._lock:
            snapshot = self._snapshots.get(snapshot_id)
            if snapshot is None:
                return None
            path = os.path.join(self.snapshots_root, snapshot_id, "image.bin")
            try:
                handle = open(path, "rb")
            except OSError:
                return None
        return _FileChunks(handle)

    def snapshot_meta(self, snapshot_id: str) -> Optional[dict]:
        with self._lock:
            snapshot = self._snapshots.get(snapshot_id)
            return dict(snapshot) if snapshot is not None else None

    def snapshot_missing_list(self, snapshot_id: str) -> Optional[dict]:
        with self._lock:
            snapshot = self._snapshots.get(snapshot_id)
            if snapshot is None:
                return None
            return {
                "snapshot_id": snapshot["snapshot_id"],
                "created_at": snapshot["created_at"],
                "sector_count": snapshot["sector_count"],
                "sector_size": SECTOR_SIZE,
                "sample_sha256": snapshot["sample_sha256"],
                "missing_count": len(snapshot["missing"]),
                "missing": snapshot["missing"],
            }

    def _load_snapshots(self) -> None:
        """重启后从磁盘恢复已有快照，保证同任务的旧快照仍可下载。"""
        os.makedirs(self.snapshots_root, exist_ok=True)
        self._snapshots.clear()
        self._snapshot_counter = 0
        for name in sorted(os.listdir(self.snapshots_root)):
            path = os.path.join(self.snapshots_root, name)
            if name.endswith(TMP_SUFFIX):
                _remove_tree(path)
                continue
            if not os.path.isdir(path) or not _SNAPSHOT_ID_RE.match(name):
                continue
            meta_path = os.path.join(path, "meta.json")
            image_path = os.path.join(path, "image.bin")
            if not (os.path.exists(meta_path) and os.path.exists(image_path)):
                continue
            try:
                with open(meta_path, "r", encoding="utf-8") as fh:
                    meta = json.load(fh)
                image_size = os.path.getsize(image_path)
            except (OSError, ValueError):
                continue
            # 只恢复当前任务的快照：样盘指纹与扇区范围都要对得上。
            if (
                meta.get("sample_sha256") != self._state.get("sample_sha256")
                or meta.get("sector_count") != self._state.get("sector_count")
                or image_size != meta.get("sector_count", 0) * SECTOR_SIZE
            ):
                continue
            self._snapshots[name] = meta
            self._snapshot_counter = max(
                self._snapshot_counter, int(_SNAPSHOT_ID_RE.match(name).group(1))
            )

    def _clear_snapshot_files(self) -> None:
        _remove_tree(self.snapshots_root)
        os.makedirs(self.snapshots_root, exist_ok=True)

    # ------------------------------------------------------------------ #
    def reset(self) -> None:
        """删除整个任务目录下的任务数据（供测试与重新建任务使用）。"""
        with self._lock:
            import shutil

            self._snapshots.clear()
            self._snapshot_counter = 0
            if os.path.isdir(self.sectors_root):
                shutil.rmtree(self.sectors_root)
            _remove_tree(self.snapshots_root)
            _unlink_quiet(self.job_path)
            _unlink_quiet(self.sample_path)
            os.makedirs(self.sectors_root, exist_ok=True)
            os.makedirs(self.snapshots_root, exist_ok=True)
            self._state = self._load_or_init()


def _fsync_dir(path: str) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class _FileChunks:
    """把已打开的只读文件按块迭代；迭代结束、关闭或释放时关掉句柄。"""

    def __init__(self, handle):
        self._handle = handle
        self._closed = False

    def __iter__(self):
        return self

    def __next__(self):
        if self._closed:
            raise StopIteration
        try:
            chunk = self._handle.read(64 * 1024)
        except BaseException:
            self.close()
            raise
        if not chunk:
            self.close()
            raise StopIteration
        return chunk

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._handle.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass


def _remove_tree(path: str) -> None:
    import shutil

    shutil.rmtree(path, ignore_errors=True)


def _unlink_quiet(path: str) -> None:
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


def _utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat(timespec="seconds")
