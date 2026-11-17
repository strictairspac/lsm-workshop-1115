"""在线合并引擎的确定性验证：零依赖，python3 -m unittest 可直接运行。

覆盖验收点：
1) 跨层旧值不复活（墓碑在 tiered 中保留，full 且无快照才清）；
2) 快照创建/读取/释放，释放后仍读原值（归档表支撑），最终清理依据；
3) 合并期间继续写入与创建快照，新写入不被合并吞掉；
4) 切换前中断（pending/preparing）后恢复原演练，重试写放大不重复计；
5) 自选部分表、整批切换、Oracle 与快照隔离核对、GC 依据完整。
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.services.merge_engine import MergeEngine, new_state, DRILL_STATUSES  # noqa: E402


CFG = {'memtable_limit': 2, 'l0_trigger': 2, 'max_levels': 3, 'final_compaction': True}


def engine():
    return MergeEngine(new_state(dict(CFG)))


def feed(e, ops):
    for op, k, v in ops:
        e.write(op, k, v)


class CrossLevelOldValueTests(unittest.TestCase):
    def setUp(self):
        self.e = engine()
        # 构造多层历史：a 旧值进深层，再写墓碑；b 多次覆盖
        feed(self.e, [('put', 'a', 'old'), ('put', 'b', 'b1'),
                      ('put', 'c', 'c1'), ('delete', 'a', None),
                      ('put', 'b', 'b2'), ('put', 'd', 'd1')])

    def test_tiered_compaction_keeps_tombstone_and_no_resurrection(self):
        l0_ids = [t['id'] for t in self.e.s['levels'][0]]
        self.assertGreaterEqual(len(l0_ids), 2)
        drill = self.e.start_drill('L0-tiered', l0_ids)
        self.assertEqual(drill['kind'], 'tiered')
        self.e.prepare_drill()
        self.e.apply_drill()
        # 墓碑仍在，已删键不复活
        r = self.e.point_read('a')
        self.assertFalse(r['found'])
        self.assertTrue(r['tombstone'])
        self.assertEqual(self.e.point_read('b')['value'], 'b2')
        self.assertEqual(self.e.point_read('d')['value'], 'd1')
        # 输出表里墓碑被保留
        target = self.e.s['levels'][1][-1]
        self.assertTrue(any(row['deleted'] for row in target['rows'].values()))
        self.assertTrue(self.e.checksums()['actual_matches_oracle'])

    def test_full_compaction_drops_tombstone_only_without_snapshot(self):
        all_ids = [t['id'] for lv in self.e.s['levels'] for t in lv]
        drill = self.e.start_drill('full', all_ids, mode='full', drop_tombstones=True)
        self.assertEqual(drill['kind'], 'full')
        self.assertTrue(drill['drop_allowed'])  # 无快照
        self.e.prepare_drill()
        self.e.apply_drill()
        # 墓碑被清理，但点查 a 仍然是 not found（不复活）
        r = self.e.point_read('a')
        self.assertFalse(r['found'])
        self.assertFalse(r['tombstone'])
        self.assertEqual(self.e.current_view(), {'b': 'b2', 'c': 'c1', 'd': 'd1'})
        # full 表内无墓碑
        table = self.e.s['levels'][-1][-1]
        self.assertTrue(table['full_compaction'])
        self.assertEqual(sum(1 for x in table['rows'].values() if x['deleted']), 0)

    def test_full_compaction_keeps_tombstone_when_snapshot_alive(self):
        self.e.create_snapshot()
        all_ids = [t['id'] for lv in self.e.s['levels'] for t in lv]
        drill = self.e.start_drill('full-with-snap', all_ids, mode='full', drop_tombstones=True)
        self.assertFalse(drill['drop_allowed'])
        self.assertFalse(drill['drop_tombstones'])
        self.e.prepare_drill()
        self.e.apply_drill()
        # 有存续快照：墓碑保留
        table = self.e.s['levels'][-1][-1]
        self.assertGreaterEqual(sum(1 for x in table['rows'].values() if x['deleted']), 1)
        # Oracle 仍然一致
        self.assertTrue(self.e.checksums()['actual_matches_oracle'])

    def test_partial_selection_cross_level_rejected(self):
        # 先做一次同层 tiered 合并，造出 L1 表，形成跨层结构
        l0 = [t['id'] for t in self.e.s['levels'][0]]
        self.e.start_drill('seed-tiered', l0)
        self.e.prepare_drill()
        self.e.apply_drill()
        self.assertTrue(self.e.s['levels'][1])
        # 再写入，造出两张新的 L0 表（memtable 容量 2，4 次写入刷 2 张表）
        feed(self.e, [('put', 'e', 'e1'), ('put', 'f', 'f1'),
                      ('put', 'g', 'g1'), ('put', 'h', 'h1')])
        self.assertGreaterEqual(len(self.e.s['levels'][0]), 2)
        all_ids = [t['id'] for lv in self.e.s['levels'] for t in lv]
        l0_id = self.e.s['levels'][0][0]['id']
        l1_id = self.e.s['levels'][1][0]['id']
        cross_partial = [l0_id, l1_id]   # 真正跨层但未选全部表
        with self.assertRaises(ValueError):
            self.e.start_drill('bad-auto', cross_partial)
        with self.assertRaises(ValueError):
            self.e.start_drill('bad-full', cross_partial, mode='full')
        # 选中全部表的 full 合并应被接受
        d = self.e.start_drill('ok-full', all_ids, mode='full')
        self.assertEqual(d['kind'], 'full')

    def test_deleted_key_never_resurrects_across_sequences(self):
        # 回归集：旧值 -> 墓碑 -> 新值 -> 再墓碑
        e = engine()
        feed(e, [('put', 'x', 'v1'), ('put', 'y', 'y1'),
                 ('delete', 'x', None), ('put', 'x', 'v2'),
                 ('put', 'z', 'z1'), ('delete', 'x', None)])
        all_ids = [t['id'] for lv in e.s['levels'] for t in lv]
        e.start_drill('full', all_ids, mode='full')
        e.prepare_drill()
        e.apply_drill()
        self.assertFalse(e.point_read('x')['found'])
        self.assertEqual(e.current_view(), {'y': 'y1', 'z': 'z1'})


class SnapshotTests(unittest.TestCase):
    def test_snapshot_reads_old_value_after_later_overwrite_and_delete(self):
        e = engine()
        e.write('put', 'k', 'v1')
        e.write('put', 'm', 'm1')  # flush -> L0
        snap = e.create_snapshot()
        e.write('put', 'k', 'v2')
        e.write('delete', 'k', None)    # 覆盖 mem 中的 k
        e.write('put', 'n', 'n1')       # flush -> 另一张 L0（含 k 墓碑）
        e.write('put', 'o', 'o1')
        # 当前视图 k 已删
        self.assertFalse(e.point_read('k')['found'])
        # 快照仍读到 v1
        sr = e.snapshot_read(snap['id'], 'k')
        self.assertTrue(sr['found'])
        self.assertEqual(sr['value'], 'v1')

    def test_snapshot_holds_archived_tables_until_release(self):
        e = engine()
        feed(e, [('put', 'a', '1'), ('put', 'b', '2'),
                 ('put', 'a', '3'), ('put', 'c', '4')])
        snap = e.create_snapshot()
        l0 = [t['id'] for t in e.s['levels'][0]]
        e.start_drill('tiered', l0)
        e.prepare_drill()
        e.apply_drill()
        # 旧表被归档而非清理
        archived_ids = {t['id'] for t in e.s['archive']}
        self.assertTrue(set(l0) & archived_ids)
        # 快照仍可读
        self.assertEqual(e.snapshot_read(snap['id'], 'a')['value'], '3')
        # GC 依据：归档表 retain，且原因点名快照
        gc = {x['table_id']: x for x in e.gc_evaluate()}
        for tid in l0:
            self.assertEqual(gc[tid]['decision'], 'retain')
            self.assertIn('快照', gc[tid]['reason'])
        # 释放后归档表被清理
        e.release_snapshot(snap['id'])
        remaining = {t['id'] for t in e.s['archive']}
        self.assertFalse(set(l0) & remaining)
        gc2 = {x['table_id']: x for x in e.gc_evaluate()}
        for tid in l0:
            self.assertNotIn(tid, gc2)

    def test_snapshot_release_triggers_tombstone_rewrite(self):
        e = engine()
        feed(e, [('put', 'a', '1'), ('put', 'b', '2'),
                 ('delete', 'a', None), ('put', 'c', '3')])
        snap = e.create_snapshot()
        all_ids = [t['id'] for lv in e.s['levels'] for t in lv]
        e.start_drill('full', all_ids, mode='full')
        e.prepare_drill()
        e.apply_drill()  # 有快照，墓碑保留
        full_table = e.s['levels'][-1][-1]
        self.assertTrue(full_table['rows']['a']['deleted'])
        before_rewrite = e.s['counters']['gc_rewrite_writes']
        e.release_snapshot(snap['id'])
        self.assertGreater(e.s['counters']['gc_rewrite_writes'], before_rewrite)
        # 重写后墓碑消失，但 a 不复活
        new_table = e.s['levels'][-1][-1]
        self.assertNotIn('a', new_table['rows'])
        self.assertFalse(e.point_read('a')['found'])
        # 重写条目不计入合并写放大（独立口径 gc_rewrite_writes）
        c = e.s['counters']
        self.assertEqual(e.write_amplification(),
                         round((c['flush_writes'] + c['compaction_writes'])
                               / max(e.s['user_ops'], 1), 4))

    def test_snapshot_limit(self):
        e = engine()
        for _ in range(50):
            e.create_snapshot()
        self.assertEqual(len(e.s['snapshots']), 50)
        with self.assertRaises(ValueError):
            e.create_snapshot()


class ConcurrentWriteDuringCompactionTests(unittest.TestCase):
    def test_writes_during_prepare_and_before_commit_are_not_swallowed(self):
        e = engine()
        feed(e, [('put', 'a', '1'), ('put', 'b', '2'),
                 ('put', 'c', '3'), ('put', 'd', '4')])
        l0 = [t['id'] for t in e.s['levels'][0]]
        drill = e.start_drill('tiered', l0)
        e.prepare_drill()
        self.assertEqual(drill['status'], 'ready')
        # 预构后、切换前继续写入并创建快照（写入只进 mem/新 L0）
        snap = e.create_snapshot()
        feed(e, [('put', 'e', '5'), ('put', 'a', '99'),
                 ('put', 'f', '6'), ('put', 'g', '7')])
        self.assertEqual(e.point_read('a')['value'], '99')
        self.assertEqual(e.point_read('g')['value'], '7')
        # 切换成功，新写入不被吞
        e.apply_drill()
        self.assertEqual(e.point_read('a')['value'], '99')
        self.assertEqual(e.point_read('e')['value'], '5')
        self.assertEqual(e.point_read('g')['value'], '7')
        # 快照隔离：snap 创建时刻 a=1
        self.assertEqual(e.snapshot_read(snap['id'], 'a')['value'], '1')
        self.assertTrue(e.checksums()['actual_matches_oracle'])

    def test_concurrent_drill_rejected_until_current_finishes(self):
        e = engine()
        e.write('put', 'a', '1')
        e.write('put', 'b', '2')
        ids = [t['id'] for t in e.s['levels'][0]]
        e.start_drill('one', ids)
        e.write('put', 'c', '3')
        e.write('put', 'd', '4')
        with self.assertRaises(ValueError):
            e.start_drill('two', [t['id'] for t in e.s['levels'][0]])


class InterruptAndIdempotencyTests(unittest.TestCase):
    def _ready_drill(self, e, name):
        feed(e, [('put', 'a', '1'), ('put', 'b', '2'),
                 ('put', 'c', '3'), ('put', 'd', '4')])
        ids = [t['id'] for t in e.s['levels'][0]]
        d = e.start_drill(name, ids)
        e.prepare_drill()
        return d, ids

    def test_ready_survives_restart_and_apply_counts_once(self):
        # ready 之后、apply 之前 worker 崩溃：状态持久化，重启 recover 不回退
        e = engine()
        d, ids = self._ready_drill(e, 'd1')
        self.assertIsNone(e.recover())
        self.assertEqual(d['status'], 'ready')
        e.apply_drill()
        c = e.s['counters']
        self.assertEqual(c['compactions'], 1)
        # 输出条目恰好计一次
        output = len(e.s['levels'][1][-1]['rows'])
        self.assertEqual(c['compaction_writes'], output)

    def test_preparing_crash_recovers_to_pending_and_retry_does_not_double_count(self):
        e = engine()
        d, ids = self._ready_drill(e, 'd2')
        output_entries = d['output_entries']
        # 模拟进程在 preparing 阶段崩溃（shadow 未成型）
        d['status'] = 'preparing'
        d['shadow'] = None
        recovered = e.recover()
        self.assertEqual(recovered['status'], 'pending')
        # 重试预构不产生任何写放大
        self.assertEqual(e.s['counters']['compaction_writes'], 0)
        e.prepare_drill()
        self.assertEqual(e.s['counters']['compaction_writes'], 0)
        e.apply_drill()
        c = e.s['counters']
        self.assertEqual(c['compactions'], 1)
        self.assertEqual(c['compaction_writes'], output_entries)
        # 写放大公式与计数一致
        self.assertEqual(e.write_amplification(),
                         round((c['flush_writes'] + c['compaction_writes'])
                               / max(e.s['user_ops'], 1), 4))

    def test_apply_rejects_when_selection_changed_and_drill_fails_without_effect(self):
        e = engine()
        feed(e, [('put', 'a', '1'), ('put', 'b', '2'),
                 ('put', 'c', '3'), ('put', 'd', '4')])
        ids = [t['id'] for t in e.s['levels'][0]]
        e.start_drill('d', ids)
        e.prepare_drill()
        levels_before = [len(lv) for lv in e.s['levels']]
        # 人为破坏：把所选表从活跃层级移除，模拟结构变更
        removed = e.s['levels'][0].pop(0)
        with self.assertRaises(ValueError):
            e.apply_drill()
        self.assertIsNone(e.s['drill'])
        self.assertEqual(e.s['drill_history'][-1]['status'], 'failed')
        # 除被移除表外，其余层级未被改动
        self.assertEqual([len(lv) for lv in e.s['levels']][1:], levels_before[1:])
        e.s['levels'][0].insert(0, removed)

    def test_status_enum_complete(self):
        self.assertEqual(set(DRILL_STATUSES),
                         {'pending', 'preparing', 'ready', 'effective', 'failed'})


class GCEvidenceAndMetricsTests(unittest.TestCase):
    def test_gc_evaluate_covers_retain_reasons(self):
        e = engine()
        feed(e, [('put', 'a', '1'), ('put', 'b', '2'),
                 ('delete', 'a', None), ('put', 'c', '3')])
        snap = e.create_snapshot()
        rows = e.gc_evaluate()
        self.assertTrue(rows)
        for row in rows:
            self.assertIn(row['decision'], ('retain', 'reclaimable'))
            self.assertTrue(row['reason'])
        # 快照引用的表 reason 必须点名快照 id
        referenced = [r for r in rows if r['snapshot_refs']]
        self.assertTrue(referenced)
        e.release_snapshot(snap['id'])

    def test_write_amplification_only_counts_flush_and_compaction(self):
        e = engine()
        for i in range(6):
            e.write('put', f'k{i}', str(i))
        c = e.s['counters']
        expected = round((c['flush_writes'] + c['compaction_writes'])
                         / max(e.s['user_ops'], 1), 4)
        self.assertEqual(e.write_amplification(), expected)
        ids = [t['id'] for t in e.s['levels'][0]][:2]
        e.start_drill('m', ids)
        e.prepare_drill()
        mid = e.write_amplification()
        e.apply_drill()
        self.assertGreaterEqual(e.write_amplification(), mid)

    def test_read_amplification_uses_fixed_probe_set(self):
        e = engine()
        e.write('put', 'a', '1')
        e.write('put', 'b', '2')
        r1 = e.read_amplification()
        e.create_snapshot()
        r2 = e.read_amplification()
        # 探测集合 = 输入键 + 固定不存在探针，不受快照影响
        self.assertEqual(r1['probe_count'], r2['probe_count'])
        self.assertEqual(r1['probe_count'], 3)


class PersistenceAndConcurrencyTests(unittest.TestCase):
    def test_state_json_roundtrip_preserves_snapshot_and_drill(self):
        import json
        e = engine()
        feed(e, [('put', 'a', '1'), ('put', 'b', '2'),
                 ('delete', 'a', None), ('put', 'c', '3')])
        snap = e.create_snapshot()
        e.start_drill('d', [t['id'] for t in e.s['levels'][0]])
        e.prepare_drill()
        # 模拟整存到 MySQL JSON 字段后再读回
        restored = MergeEngine(json.loads(json.dumps(e.s)))
        self.assertEqual(restored.snapshot_read(snap['id'], 'b')['value'], '2')
        self.assertEqual(restored.s['drill']['status'], 'ready')
        # 读回后仍可完成切换，且只计一次写放大
        restored.apply_drill()
        self.assertEqual(restored.s['counters']['compactions'], 1)
        self.assertFalse(restored.point_read('a')['found'])

    def test_snapshot_isolation_across_multiple_compactions_with_archive(self):
        # 快照在第一次合并前创建，连续多次合并后快照仍读原值（覆盖历史归档表场景）
        e = engine()
        feed(e, [('put', 'a', 'v1'), ('put', 'b', 'b1')])
        snap = e.create_snapshot()
        # 第一次 tiered 合并：旧表因快照引用进入归档
        l0 = [t['id'] for t in e.s['levels'][0]]
        e.start_drill('c1', l0)
        e.prepare_drill()
        e.apply_drill()
        self.assertTrue(e.s['archive'])
        # 再写入并做第二次合并
        feed(e, [('put', 'c', 'c1'), ('put', 'd', 'd1'),
                 ('put', 'e', 'e1'), ('put', 'f', 'f1')])
        l0b = [t['id'] for t in e.s['levels'][0]]
        e.start_drill('c2', l0b)
        e.prepare_drill()
        e.apply_drill()
        # 快照仍读到创建时刻的值
        self.assertEqual(e.snapshot_read(snap['id'], 'a')['value'], 'v1')
        self.assertEqual(e.snapshot_read(snap['id'], 'b')['value'], 'b1')
        # 释放快照后归档表被清理
        e.release_snapshot(snap['id'])
        self.assertEqual(e.s['archive'], [])

    def test_failed_apply_leaves_state_untouched(self):
        e = engine()
        feed(e, [('put', 'a', '1'), ('put', 'b', '2'),
                 ('put', 'c', '3'), ('put', 'd', '4')])
        ids = [t['id'] for t in e.s['levels'][0]]
        e.start_drill('d', ids)
        e.prepare_drill()
        levels_before = [len(lv) for lv in e.s['levels']]
        counters_before = dict(e.s['counters'])
        archive_before = len(e.s['archive'])
        # 破坏所选表结构后 apply 必须失败且无副作用
        removed = e.s['levels'][0].pop(0)
        with self.assertRaises(ValueError):
            e.apply_drill()
        self.assertIsNone(e.s['drill'])
        self.assertEqual(e.s['drill_history'][-1]['status'], 'failed')
        self.assertEqual(len(e.s['archive']), archive_before)
        self.assertEqual(e.s['counters'], counters_before)
        # 除被测试主动移除的表外，其他层级未变化
        self.assertEqual([len(lv) for lv in e.s['levels']][1:], levels_before[1:])
        e.s['levels'][0].insert(0, removed)

    def test_fresh_pending_not_marked_recovered_but_still_preparable(self):
        import json
        e = engine()
        feed(e, [('put', 'a', '1'), ('put', 'b', '2')])
        e.start_drill('p', [t['id'] for t in e.s['levels'][0]])  # 全新 pending
        restored = MergeEngine(json.loads(json.dumps(e.s)))
        # 全新 pending 不触发恢复事件
        self.assertIsNone(restored.recover())
        self.assertEqual(restored.s['drill']['status'], 'pending')
        restored.prepare_drill()
        restored.apply_drill()
        self.assertEqual(restored.s['counters']['compactions'], 1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
