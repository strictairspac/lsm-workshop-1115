"""可持续写入的在线 LSM 合并演练引擎（零第三方依赖，纯 Python 确定性模型）。

与一次性整批演算的 lsm_engine.simulate 不同，本引擎把存储状态保存在可 JSON 序列化的
state 字典中，由 MySQL 整存，支持：

- 持续 put/delete 写入与 memtable 刷盘（在线引擎不自动做层级合并，只提示 L0 超阈值）；
- 创建/释放读取快照，快照持有 memtable 副本与 SSTable 引用，快照读永远读创建时的值；
- 自选部分 SSTable 发起合并演练：同层=tiered（始终保留墓碑），跨层必须选全部表=full
  （仅当没有存续快照需要旧历史时才允许清理墓碑）；
- 演练两阶段：先在 shadow 区预构结果（pending/preparing/ready），再整批切换生效
  （effective）。切换前中断可恢复、可重试，写放大只在真正 apply 时计一次；
- 旧表/墓碑/被覆盖旧值的保留与清理都产出可核对依据（gc_log + gc_evaluate）。

本模块不 import 任何第三方库，便于用标准库 unittest 直接验证。
"""
import hashlib
import json

# 演练状态机：pending 待合并 -> preparing 预构中 -> ready 待切换 -> effective 已生效 / failed
DRILL_STATUSES = ('pending', 'preparing', 'ready', 'effective', 'failed')
PROBE_KEY = '__missing_probe__'


def _sha(payload):
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def new_state(config):
    """根据存储配置创建一份空的在线引擎状态。"""
    capacity = config['memtable_limit']
    trigger = config['l0_trigger']
    depth = config['max_levels']
    if not 2 <= capacity <= 64 or not 2 <= trigger <= 8 or not 2 <= depth <= 6:
        raise ValueError('层级配置超出允许范围')
    return {
        'version': 1,
        'config': {
            'memtable_limit': capacity,
            'l0_trigger': trigger,
            'max_levels': depth,
            'final_compaction': bool(config.get('final_compaction', True)),
        },
        'seq': 0,
        'user_ops': 0,
        'next_table_id': 1,
        'next_snapshot_id': 1,
        'mem': {},                              # 活跃 memtable：key -> row
        'levels': [[] for _ in range(depth)],   # 每层是不可变 SSTable 列表
        'snapshots': [],                        # 存续读取快照
        'archive': [],                          # 被合并移除但仍被快照引用的旧表
        'purged': [],                           # 已清理表的审计记录
        'drill': None,                          # 当前未完成演练（同一时刻仅一个）
        'drill_history': [],                    # 已结束演练（effective/failed）
        'counters': {
            'flushes': 0,
            'compactions': 0,
            'flush_writes': 0,
            'compaction_writes': 0,
            'gc_rewrite_writes': 0,
        },
        'timeline': [],
        'gc_log': [],
        'oracle': {},           # 最后写入者字典（活值），用于核对
        'all_keys': [],         # 出现过的全部键（有序）
    }


class MergeEngine:
    """对一份 state 字典提供全部在线操作；方法内只读写 self.state，便于整存与恢复。"""

    def __init__(self, state):
        self.state = state
        self.s = state

    # ---------- 基础工具 ----------
    def _event(self, event, **fields):
        item = {'step': len(self.s['timeline']) + 1, 'event': event}
        item.update(fields)
        item['level_table_counts'] = [len(lv) for lv in self.s['levels']]
        self.s['timeline'].append(item)
        return item

    def _gc_record(self, action, table_id, reason, _defer=False, **fields):
        item = {'step': len(self.s['gc_log']) + 1, 'action': action,
                'table_id': table_id, 'reason': reason}
        item.update(fields)
        if not _defer:
            self.s['gc_log'].append(item)
        return item

    def _all_tables(self):
        """活跃层级 + 归档中的全部不可变表。"""
        return [t for lv in self.s['levels'] for t in lv] + list(self.s['archive'])

    def _table_map(self):
        return {t['id']: t for t in self._all_tables()}

    @staticmethod
    def _row(seq, op, value):
        if op not in ('put', 'delete'):
            raise ValueError('操作或键不合法')
        return {'seq': seq, 'value': None if op == 'delete' else value,
                'deleted': op == 'delete'}

    # ---------- 持续写入 ----------
    def write(self, op, key, value=None):
        if not isinstance(key, str) or not 1 <= len(key) <= 64:
            raise ValueError('键长度必须在1到64之间')
        if op not in ('put', 'delete'):
            raise ValueError('操作或键不合法')
        if op == 'put' and (not isinstance(value, str) or len(value) > 200):
            raise ValueError('put必须包含不超过200字的字符串')
        self.s['seq'] += 1
        row = self._row(self.s['seq'], op, value)
        self.s['mem'][key] = row
        self.s['user_ops'] += 1
        if op == 'delete':
            self.s['oracle'].pop(key, None)
        else:
            self.s['oracle'][key] = value
        if key not in self.s['all_keys']:
            self.s['all_keys'] = sorted(self.s['all_keys'] + [key])
        hint = None
        if len(self.s['mem']) >= self.s['config']['memtable_limit']:
            self._flush()
            if len(self.s['levels'][0]) >= self.s['config']['l0_trigger']:
                hint = 'L0 表数已达触发值，可发起合并演练（在线引擎不会自动合并）'
        return {'seq': row['seq'], 'key': key, 'hint': hint}

    def bulk_write(self, entries):
        if not 1 <= len(entries) <= 2000:
            raise ValueError('每次批量操作必须在1到2000之间')
        out = []
        for e in entries:
            out.append(self.write(e['op'], e['key'], e.get('value')))
        return out

    def _flush(self):
        if not self.s['mem']:
            return
        tid = self.s['next_table_id']
        self.s['next_table_id'] += 1
        rows = {k: dict(v) for k, v in sorted(self.s['mem'].items())}
        table = {'id': tid, 'level': 0, 'origin': 'flush',
                 'full_compaction': False, 'created_seq': self.s['seq'], 'rows': rows}
        self.s['levels'][0].append(table)
        self.s['counters']['flushes'] += 1
        self.s['counters']['flush_writes'] += len(rows)
        self.s['mem'] = {}
        self._event('memtable_flush', level=0, table_id=tid, output_entries=len(rows))

    # ---------- 点查（当前视图，带范围命中证据） ----------
    @staticmethod
    def _best(candidates):
        """candidates: 可迭代 row（dict 含 seq），返回序号最大者。"""
        best = None
        for row in candidates:
            if row is not None and (best is None or row['seq'] > best['seq']):
                best = row
        return best

    def point_read(self, key):
        evidence = []
        candidates = []
        if key in self.s['mem']:
            candidates.append(self.s['mem'][key])
            evidence.append({'source': 'memtable', 'contained': True, 'seq': self.s['mem'][key]['seq']})
        for lv in self.s['levels']:
            for t in lv:
                rows = t['rows']
                in_range = bool(rows) and min(rows) <= key <= max(rows)
                if in_range:   # 无 Bloom 模型：范围命中即读一次 SSTable
                    hit = rows.get(key)
                    evidence.append({'source': 'sstable', 'table_id': t['id'],
                                     'level': t['level'], 'contained': hit is not None,
                                     'seq': hit['seq'] if hit else None})
                    if hit:
                        candidates.append(hit)
        best = self._best(candidates)
        return self._read_result(key, best, evidence)

    def snapshot_read(self, snapshot_id, key):
        snap = self._require_snapshot(snapshot_id)
        table_map = self._table_map()
        evidence = []
        candidates = []
        if key in snap['mem']:
            candidates.append(snap['mem'][key])
            evidence.append({'source': 'snapshot_memtable', 'contained': True,
                             'seq': snap['mem'][key]['seq']})
        for tid in snap['tables']:
            t = table_map.get(tid)
            if not t:
                continue
            rows = t['rows']
            in_range = bool(rows) and min(rows) <= key <= max(rows)
            if in_range:
                hit = rows.get(key)
                evidence.append({'source': 'snapshot_sstable', 'table_id': tid,
                                 'level': t['level'], 'contained': hit is not None,
                                 'seq': hit['seq'] if hit else None})
                if hit:
                    candidates.append(hit)
        best = self._best(candidates)
        result = self._read_result(key, best, evidence)
        result['snapshot_id'] = snapshot_id
        return result

    @staticmethod
    def _read_result(key, best, evidence):
        touched = sum(1 for e in evidence if e['source'] != 'memtable'
                      and e['source'] != 'snapshot_memtable')
        if best is None:
            return {'key': key, 'found': False, 'value': None, 'tombstone': False,
                    'seq': None, 'tables_touched': touched, 'evidence': evidence}
        if best['deleted']:
            return {'key': key, 'found': False, 'value': None, 'tombstone': True,
                    'seq': best['seq'], 'tables_touched': touched, 'evidence': evidence}
        return {'key': key, 'found': True, 'value': best['value'], 'tombstone': False,
                'seq': best['seq'], 'tables_touched': touched, 'evidence': evidence}

    # ---------- 读取快照 ----------
    def _require_snapshot(self, snapshot_id):
        for snap in self.s['snapshots']:
            if snap['id'] == snapshot_id:
                return snap
        raise KeyError(f'快照 {snapshot_id} 不存在或已释放')

    def _snapshot_view(self, snap):
        """快照创建时刻的完整存活键值视图（用于校验和与快照隔离核对）。"""
        view = {}
        for key in snap['keys']:
            r = self.snapshot_read(snap['id'], key)
            if r['found']:
                view[key] = r['value']
        return dict(sorted(view.items()))

    def create_snapshot(self):
        if len(self.s['snapshots']) >= 50:
            raise ValueError('存续快照上限50个，请先释放旧快照')
        sid = self.s['next_snapshot_id']
        self.s['next_snapshot_id'] += 1
        table_ids = [t['id'] for lv in self.s['levels'] for t in lv]
        snap = {
            'id': sid,
            'created_seq': self.s['seq'],
            'mem': {k: dict(v) for k, v in self.s['mem'].items()},
            'tables': table_ids,
            'keys': list(self.s['all_keys']),
        }
        self.s['snapshots'].append(snap)
        snap['checksum'] = _sha(self._snapshot_view(snap))
        self._event('snapshot_created', snapshot_id=sid, created_seq=snap['created_seq'],
                    table_refs=len(table_ids), mem_keys=len(snap['mem']))
        return snap

    def release_snapshot(self, snapshot_id):
        snap = self._require_snapshot(snapshot_id)
        refs = set(snap['tables'])
        self.s['snapshots'] = [x for x in self.s['snapshots'] if x['id'] != snapshot_id]
        self._event('snapshot_released', snapshot_id=snapshot_id)
        self._gc_sweep()
        return {'released': snapshot_id, 'freed_table_refs': sorted(refs)}

    # ---------- 垃圾回收：保留/清理依据 ----------
    def _live_snapshot_table_ids(self):
        ids = set()
        for snap in self.s['snapshots']:
            ids.update(snap['tables'])
        return ids

    def _gc_sweep(self):
        """快照释放后调用：归档表无引用则清理；无快照引用的 full 表重写清墓碑。"""
        live_refs = self._live_snapshot_table_ids()
        # 1) 归档表：只有在没有任何存续快照引用时才真正清理
        for t in list(self.s['archive']):
            if t['id'] not in live_refs:
                self.s['archive'].remove(t)
                self.s['purged'].append({'table_id': t['id'], 'reason': '快照已释放，归档表无存续引用'})
                self._gc_record('purge_archived_table', t['id'],
                                '所有引用该表的快照均已释放，且其不在活跃层级，可安全清理')
        # 2) full 表：无存续快照引用时，重写移除墓碑（tiered 表永远不在此清墓碑）
        for lv in self.s['levels']:
            for t in list(lv):
                if not t['full_compaction'] or t['id'] in live_refs:
                    continue
                tombs = [k for k, r in t['rows'].items() if r['deleted']]
                if not tombs:
                    continue
                new_id = self.s['next_table_id']
                self.s['next_table_id'] += 1
                rows = {k: dict(r) for k, r in sorted(t['rows'].items()) if not r['deleted']}
                new_table = {'id': new_id, 'level': t['level'], 'origin': 'gc_rewrite',
                             'full_compaction': True, 'created_seq': self.s['seq'], 'rows': rows}
                lv[lv.index(t)] = new_table
                self.s['counters']['gc_rewrite_writes'] += len(rows)
                self.s['purged'].append({'table_id': t['id'], 'reason': 'full 表墓碑在快照释放后重写清理'})
                self._gc_record('rewrite_full_table_drop_tombstones', new_id,
                                '无存续快照引用该 full 表，墓碑与被删键不再被需要，重写为纯活值表',
                                old_table_id=t['id'], dropped_tombstones=len(tombs),
                                output_entries=len(rows))

    def gc_evaluate(self):
        """逐表给出保留或清理依据，供页面核对（不改变状态）。"""
        live_refs = self._live_snapshot_table_ids()
        result = []
        for t in self._all_tables():
            snap_ids = sorted(s['id'] for s in self.s['snapshots'] if t['id'] in s['tables'])
            tombs = sum(1 for r in t['rows'].values() if r['deleted'])
            in_archive = t in self.s['archive']
            if in_archive:
                if snap_ids:
                    decision, reason = 'retain', f'归档旧表仍被存续快照 {snap_ids} 引用，释放前不可清理'
                else:
                    decision, reason = 'reclaimable', '归档表已无快照引用，下一次 GC 扫描将清理'
            elif t['full_compaction']:
                if snap_ids:
                    decision, reason = 'retain', f'full 表被快照 {snap_ids} 引用，墓碑须保留以支撑快照读'
                elif tombs:
                    decision, reason = 'reclaimable', '无存续快照引用，下一次 GC 扫描将重写并清理墓碑'
                else:
                    decision, reason = 'retain', 'full 表仅含活值，无需清理'
            else:
                decision, reason = 'retain', 'tiered/flush 表按策略始终保留墓碑与层级，仅 full 合并可清理'
            result.append({'table_id': t['id'], 'level': t['level'], 'origin': t['origin'],
                           'full_compaction': t['full_compaction'], 'entries': len(t['rows']),
                           'tombstones': tombs, 'archived': in_archive,
                           'snapshot_refs': snap_ids, 'decision': decision, 'reason': reason})
        return result

    # ---------- 合并演练 ----------
    def start_drill(self, name, selected_ids, mode='auto', drop_tombstones=True):
        """发起合并演练。

        mode:
        - tiered：同层局部合并，只允许选同一层的表，结果写入下一层，始终保留墓碑；
        - full：完整历史合并，必须选中当前全部 SSTable，结果写入最深层，
          仅当没有存续快照时才允许清理墓碑；
        - auto（默认）：同层选择按 tiered；选中全部表且跨层时按 full。
        """
        if self.s['drill'] is not None and self.s['drill']['status'] in ('pending', 'preparing', 'ready'):
            raise ValueError('已有未完成的合并演练，请先切换生效或中断后再创建')
        if not selected_ids:
            raise ValueError('请至少选择一张 SSTable')
        active = {t['id']: t for lv in self.s['levels'] for t in lv}
        try:
            selected = [active[i] for i in selected_ids]
        except KeyError:
            raise ValueError('所选表中包含已不存在的 SSTable，请刷新后重试')
        levels_used = sorted({t['level'] for t in selected})
        all_active_ids = sorted(active)
        if mode == 'tiered':
            if len(levels_used) != 1:
                raise ValueError('tiered 同层合并只能选择同一层的 SSTable')
            kind = 'tiered'
        elif mode == 'full':
            if sorted(selected_ids) != all_active_ids:
                raise ValueError('full 完整历史合并必须选中当前全部 SSTable')
            kind = 'full'
        elif mode == 'auto':
            if len(levels_used) == 1:
                kind = 'tiered'
            elif sorted(selected_ids) == all_active_ids:
                kind = 'full'
            else:
                raise ValueError('跨层合并必须选中当前全部 SSTable（完整历史合并）；同层局部合并请只选同一层的表')
        else:
            raise ValueError('合并模式必须是 tiered、full 或 auto')
        if kind == 'tiered':
            source_level = levels_used[0]
            target_level = min(source_level + 1, self.s['config']['max_levels'] - 1)
        else:
            source_level = None
            target_level = self.s['config']['max_levels'] - 1
        drop_allowed = len(self.s['snapshots']) == 0
        drill = {
            'id': len(self.s['drill_history']) + 1,
            'name': name,
            'kind': kind,
            'source_level': source_level,
            'target_level': target_level,
            'selected_ids': list(selected_ids),
            'drop_requested': bool(drop_tombstones),
            'drop_allowed': drop_allowed,
            'drop_tombstones': bool(drop_tombstones) and drop_allowed and kind == 'full',
            'status': 'pending',
            'phase': 'queued',
            'shadow': None,
            'created_seq': self.s['seq'],
            'write_amplification_before': self.write_amplification(),
            'read_amp_before': self.read_amplification(),
        }
        self.s['drill'] = drill
        self._event('drill_created', drill_id=drill['id'], kind=kind,
                    selected_tables=list(selected_ids), target_level=target_level,
                    drop_tombstones=drill['drop_tombstones'])
        return drill

    @staticmethod
    def _merge_tables(tables, drop_tombstones):
        latest = {}
        for t in tables:
            for k, r in t['rows'].items():
                if k not in latest or latest[k]['seq'] < r['seq']:
                    latest[k] = dict(r)
        merged = {k: latest[k] for k in sorted(latest)
                  if not (drop_tombstones and latest[k]['deleted'])}
        return merged

    def prepare_drill(self):
        """worker 调用：在 shadow 区预构合并结果。幂等，已预构不重复计算/计数。"""
        drill = self.s['drill']
        if drill is None:
            raise ValueError('当前没有合并演练')
        if drill['status'] == 'ready':
            return drill
        if drill['status'] not in ('pending', 'preparing'):
            raise ValueError(f'演练状态 {drill["status"]} 不可预构')
        drill['status'] = 'preparing'
        drill['phase'] = 'building_shadow'
        self._event('drill_prepare_started', drill_id=drill['id'])
        table_map = self._table_map()
        selected = [table_map[i] for i in drill['selected_ids']]
        merged = self._merge_tables(selected, drill['drop_tombstones'])
        shadow_id = self.s['next_table_id']
        self.s['next_table_id'] += 1
        drill['shadow'] = {
            'id': shadow_id,
            'level': drill['target_level'],
            'origin': 'full' if drill['kind'] == 'full' else 'tiered',
            'full_compaction': drill['kind'] == 'full',
            'created_seq': self.s['seq'],
            'rows': merged,
        }
        # 预构阶段先核对：shadow 对所选范围的读结果必须与所选表一致（不写任何放大计数）
        self._verify_shadow_scope(drill, selected, merged)
        drill['status'] = 'ready'
        drill['phase'] = 'awaiting_commit'
        drill['output_entries'] = len(merged)
        drill['tombstones_in_output'] = sum(1 for r in merged.values() if r['deleted'])
        self._event('drill_prepared', drill_id=drill['id'], shadow_table_id=shadow_id,
                    output_entries=len(merged), drop_tombstones=drill['drop_tombstones'])
        return drill

    def _verify_shadow_scope(self, drill, selected, merged):
        # 所选表覆盖的每个键：合并后最高序号记录必须与 shadow 中记录一致
        latest = {}
        for t in selected:
            for k, r in t['rows'].items():
                if k not in latest or latest[k]['seq'] < r['seq']:
                    latest[k] = r
        for k, r in latest.items():
            if drill['drop_tombstones'] and r['deleted']:
                if k in merged:
                    raise ValueError('预构校验失败：应清理的墓碑仍出现在 shadow 中')
                continue
            if k not in merged or merged[k]['seq'] != r['seq']:
                raise ValueError('预构校验失败：shadow 与所选表最高序号记录不一致')

    def _point_read_with_levels(self, key, levels):
        """在给定层级（含当前 memtable）上做点查，用于切换前的无副作用校验。"""
        evidence_seq = []
        candidates = []
        if key in self.s['mem']:
            candidates.append(self.s['mem'][key])
        for lv in levels:
            for t in lv:
                rows = t['rows']
                if rows and min(rows) <= key <= max(rows):
                    hit = rows.get(key)
                    if hit:
                        candidates.append(hit)
                        evidence_seq.append(hit['seq'])
        best = self._best(candidates)
        if best is None or best['deleted']:
            return None
        return best['value']

    def _snapshot_view_with_levels(self, snap, levels, table_map):
        view = {}
        for key in snap['keys']:
            candidates = []
            if key in snap['mem']:
                candidates.append(snap['mem'][key])
            for tid in snap['tables']:
                t = table_map.get(tid)
                if not t:
                    continue
                rows = t['rows']
                if rows and min(rows) <= key <= max(rows):
                    hit = rows.get(key)
                    if hit:
                        candidates.append(hit)
            best = self._best(candidates)
            if best and not best['deleted']:
                view[key] = best['value']
        return dict(sorted(view.items()))

    def apply_drill(self):
        """整批切换：仅在 ready 时可调用；真正写放大只在此处计一次，重试幂等。

        所有校验都在临时结构上完成，全部通过后才一次性落地 levels/archive/gc_log，
        保证切换的原子性：任何校验失败都不会留下半切换状态。
        """
        drill = self.s['drill']
        if drill is None:
            raise ValueError('当前没有合并演练')
        if drill['status'] == 'effective':
            return drill
        if drill['status'] != 'ready' or not drill['shadow']:
            raise ValueError(f'演练状态 {drill["status"]}，尚未预构完成，不能切换')

        def fail(reason):
            drill['status'] = 'failed'
            drill['phase'] = 'commit_aborted'
            self._archive_drill()
            raise ValueError(reason)

        active = {t['id']: t for lv in self.s['levels'] for t in lv}
        if any(i not in active for i in drill['selected_ids']):
            fail('切换失败：所选表在预构后已不存在，演练中断且未生效')
        # 提交前再做一次确定性核对（新写入只进 mem/新 L0，不影响所选不可变表）
        selected = [active[i] for i in drill['selected_ids']]
        if self._merge_tables(selected, drill['drop_tombstones']) != drill['shadow']['rows']:
            fail('切换失败：shadow 与当前所选表核对不一致，演练中断且未生效')

        live_refs = self._live_snapshot_table_ids()
        shadow = dict(drill['shadow'])
        shadow['rows'] = {k: dict(r) for k, r in shadow['rows'].items()}

        # ---- 在临时结构上构建新层级，不触碰真实状态 ----
        new_levels = [[] for _ in self.s['levels']]
        for li, lv in enumerate(self.s['levels']):
            for t in lv:
                if t['id'] not in drill['selected_ids']:
                    new_levels[li].append(t)
        new_levels[drill['target_level']].append(shadow)

        # 校验 1：新层级下全部已知键点查与 Oracle 一致（已删键不复活）
        for key in self.s['all_keys']:
            value = self._point_read_with_levels(key, new_levels)
            if key in self.s['oracle']:
                if value != self.s['oracle'][key]:
                    fail('切换回滚：合并后点查值与 Oracle 不一致，疑似旧值复活或覆盖丢失')
            elif value is not None:
                fail('切换回滚：已删键在合并后复活，违反墓碑语义')
        # 校验 2：每个存续快照的视图校验和必须不变（快照隔离）
        # table_map 需覆盖：新层级表、历史归档表、本次将归档的旧表
        table_map = {t['id']: t for lv in new_levels for t in lv}
        table_map.update({t['id']: t for t in self.s['archive']})
        table_map.update({t['id']: t for t in selected})
        for snap in self.s['snapshots']:
            view = self._snapshot_view_with_levels(snap, new_levels, table_map)
            if _sha(view) != snap['checksum']:
                fail('切换回滚：存续快照视图发生变化，违反快照隔离')

        # ---- 全部校验通过，一次性落地 ----
        self.s['levels'] = new_levels
        new_archive = list(self.s['archive'])
        new_purged = list(self.s['purged'])
        new_gc = []
        for tid in drill['selected_ids']:
            t = active[tid]
            if tid in live_refs:
                new_archive.append(t)
                new_gc.append(self._gc_record('archive_selected_table', tid,
                                              '合并已生效，但该表仍被存续快照引用，移入归档支撑快照读',
                                              _defer=True))
            else:
                new_purged.append({'table_id': tid, 'reason': '合并消耗且无快照引用'})
                new_gc.append(self._gc_record('consume_selected_table', tid,
                                              '合并已生效，该表无存续快照引用且不在未选表中，可安全清理',
                                              _defer=True))
        self.s['archive'] = new_archive
        self.s['purged'] = new_purged
        for item in new_gc:
            item['step'] = len(self.s['gc_log']) + 1
            self.s['gc_log'].append(item)
        # 计数只在此处落地一次
        self.s['counters']['compactions'] += 1
        self.s['counters']['compaction_writes'] += len(shadow['rows'])
        drill['status'] = 'effective'
        drill['phase'] = 'committed'
        drill['read_amp_after'] = self.read_amplification()
        drill['write_amplification_after'] = self.write_amplification()
        self._event('drill_effective', drill_id=drill['id'], shadow_table_id=shadow['id'],
                    target_level=drill['target_level'], output_entries=len(shadow['rows']),
                    archived=[t['id'] for t in self.s['archive']])
        self._archive_drill()
        return drill

    def _archive_drill(self):
        drill = self.s['drill']
        self.s['drill_history'].append(drill)
        self.s['drill'] = None

    def recover(self):
        """worker 启动/空轮询调用：只恢复真正中断在 preparing 的演练。

        全新 pending（仅排队未开始）无需恢复；ready/effective 不受影响。
        shadow 只在预构成功时一次性写入，因此 preparing 中断不会留下半个 shadow，
        重置为 pending 后重试 prepare 不重复计数。
        """
        drill = self.s['drill']
        if drill and drill['status'] == 'preparing':
            drill['status'] = 'pending'
            drill['phase'] = 'recovered_waiting_prepare'
            drill['shadow'] = None
            self._event('drill_recovered', drill_id=drill['id'])
            return drill
        return None

    # ---------- 指标与视图 ----------
    def read_amplification(self):
        universe = sorted(set(self.s['all_keys']) | {PROBE_KEY})
        touches = sum(self.point_read(k)['tables_touched'] for k in universe)
        return {'probe_count': len(universe), 'table_reads': touches,
                'read_amplification': round(touches / len(universe), 4)}

    def write_amplification(self):
        c = self.s['counters']
        denom = max(self.s['user_ops'], 1)
        return round((c['flush_writes'] + c['compaction_writes']) / denom, 4)

    def current_view(self):
        return dict(sorted(self.s['oracle'].items()))

    def level_shape(self):
        out = []
        for i, lv in enumerate(self.s['levels']):
            tables = []
            for t in lv:
                rows = t['rows']
                tables.append({'id': t['id'], 'entries': len(rows),
                               'min_key': min(rows) if rows else None,
                               'max_key': max(rows) if rows else None,
                               'tombstones': sum(1 for r in rows.values() if r['deleted']),
                               'origin': t['origin'], 'full_compaction': t['full_compaction']})
            out.append({'level': i, 'tables': tables})
        return out

    def checksums(self):
        actual = self.current_view()
        return {'actual_checksum': _sha(actual),
                'oracle_checksum': _sha(dict(sorted(self.s['oracle'].items()))),
                'actual_matches_oracle': actual == dict(sorted(self.s['oracle'].items()))}

    def summary(self):
        c = self.s['counters']
        return {
            'seq': self.s['seq'], 'user_ops': self.s['user_ops'],
            'live_keys': len(self.s['oracle']), 'all_keys': len(self.s['all_keys']),
            'memtable_keys': len(self.s['mem']),
            'l0_tables': len(self.s['levels'][0]),
            'l0_trigger': self.s['config']['l0_trigger'],
            'snapshot_count': len(self.s['snapshots']),
            'archived_tables': len(self.s['archive']),
            'flushes': c['flushes'], 'compactions': c['compactions'],
            'flush_writes': c['flush_writes'], 'compaction_writes': c['compaction_writes'],
            'gc_rewrite_writes': c['gc_rewrite_writes'],
            'write_amplification': self.write_amplification(),
            **self.read_amplification(),
            **self.checksums(),
        }
