"""在线合并演练的持久化服务。

每次状态变更都在 SELECT ... FOR UPDATE 行锁内完成，把持续写入、快照、合并提交串行化，
保证并发合并不会吞掉新写入。引擎状态整存于 MergeDrill.state，worker 崩溃后可原样恢复。
"""
from fastapi import HTTPException
from sqlalchemy import select
from redis import Redis
from ..config.settings import REDIS_URL
from ..models.merge_drill import MergeDrill
from ..models.config import Config
from .config import serialize as config_dict
from .merge_engine import MergeEngine, new_state

queue = Redis.from_url(REDIS_URL)
JOB_KEY = 'merge:jobs'


def _locked(db, ident):
    """取行锁加载演练；MySQL 上 FOR UPDATE 直到提交/回滚才释放。"""
    x = db.get(MergeDrill, ident, with_for_update=True)
    if not x:
        raise HTTPException(404, '合并演练不存在')
    return x


def _engine(x):
    return MergeEngine(x.state)


def _sync_status(x, eng):
    # 行级状态：有活动演练取演练状态，否则空闲（可继续写入/发起演练）
    if eng.s['drill'] is not None:
        x.status = eng.s['drill']['status']
    elif eng.s['drill_history']:
        x.status = eng.s['drill_history'][-1]['status']
    else:
        x.status = 'idle'


def serialize(x):
    eng = MergeEngine(x.state)
    drill = eng.s['drill']
    return {
        'id': x.id, 'name': x.name, 'config_id': x.config_id,
        'status': x.status, 'last_error': x.last_error,
        'created_at': str(x.created_at), 'updated_at': str(x.updated_at),
        'summary': eng.summary(),
        'levels': eng.level_shape(),
        'snapshots': [{'id': s['id'], 'created_seq': s['created_seq'],
                       'table_refs': len(s['tables']), 'mem_keys': len(s['mem']),
                       'keys': len(s['keys']), 'checksum': s['checksum']}
                      for s in eng.s['snapshots']],
        'archive': [{'table_id': t['id'], 'level': t['level'],
                     'entries': len(t['rows']),
                     'tombstones': sum(1 for r in t['rows'].values() if r['deleted'])}
                    for t in eng.s['archive']],
        'drill': drill,
        'drill_history': [{'id': d['id'], 'name': d['name'], 'kind': d['kind'],
                           'status': d['status'], 'phase': d['phase'],
                           'output_entries': d.get('output_entries')}
                          for d in eng.s['drill_history']],
        'gc': eng.gc_evaluate(),
        'timeline': eng.s['timeline'][-30:],
        'gc_log': eng.s['gc_log'][-30:],
    }


def full_detail(x):
    """完整导出（含全部时间线/GC 日志与引擎状态摘要），供下载核对。"""
    data = serialize(x)
    eng = MergeEngine(x.state)
    data['state_policy'] = {
        'model': '教学在线 LSM 模型：Python 内存算法，MySQL 整存引擎状态，非真实存储引擎',
        'gc_rule': '旧值/墓碑仅当未选表且无存续快照引用时才清理；tiered 永远保留墓碑，'
                   'full 且无快照才可清墓碑；GC 重写条目单列 gc_rewrite_writes，不计合并写放大',
        'snapshot_rule': '快照持有 memtable 副本与 SSTable 引用，释放前旧表归档保留，快照读原值',
        'write_amp_rule': '写放大=(刷盘写出+合并写出)/用户操作数；预构不计数，仅 apply 计一次，重试不重复',
    }
    data['full_timeline'] = eng.s['timeline']
    data['full_gc_log'] = eng.s['gc_log']
    data['counters'] = eng.s['counters']
    data['checksums'] = eng.checksums()
    return data


def listing(db):
    rows = db.scalars(select(MergeDrill).order_by(MergeDrill.id.desc())).all()
    return [serialize(x) for x in rows]


def detail(db, ident):
    x = db.get(MergeDrill, ident)
    if not x:
        raise HTTPException(404, '合并演练不存在')
    return serialize(x)


def create(db, data):
    config = db.get(Config, data.config_id)
    if not config:
        raise HTTPException(404, '存储配置不存在')
    if db.scalar(select(MergeDrill).where(MergeDrill.name == data.name)):
        raise HTTPException(409, '合并演练名称已存在')
    x = MergeDrill(name=data.name, config_id=data.config_id,
                   status='idle', state=new_state(config_dict(config)))
    db.add(x)
    db.commit()
    db.refresh(x)
    return serialize(x)


def write_one(db, ident, data):
    x = _locked(db, ident)
    eng = _engine(x)
    try:
        eng.write(data.op, data.key, data.value)
    except ValueError as exc:
        raise HTTPException(422, str(exc))
    x.state = eng.s
    _sync_status(x, eng)
    db.commit()
    return serialize(x)


def bulk_write(db, ident, data):
    x = _locked(db, ident)
    eng = _engine(x)
    try:
        eng.bulk_write([e.model_dump() for e in data.entries])
    except ValueError as exc:
        raise HTTPException(422, str(exc))
    x.state = eng.s
    _sync_status(x, eng)
    db.commit()
    return serialize(x)


def create_snapshot(db, ident):
    x = _locked(db, ident)
    eng = _engine(x)
    try:
        snap = eng.create_snapshot()
    except ValueError as exc:
        raise HTTPException(422, str(exc))
    x.state = eng.s
    db.commit()
    return {'snapshot': {'id': snap['id'], 'created_seq': snap['created_seq'],
                         'checksum': snap['checksum']}, 'drill': serialize(x)}


def release_snapshot(db, ident, snapshot_id):
    x = _locked(db, ident)
    eng = _engine(x)
    try:
        eng.release_snapshot(snapshot_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc))
    x.state = eng.s
    _sync_status(x, eng)
    db.commit()
    return serialize(x)


def read_key(db, ident, key, snapshot_id=None):
    # 只读不改变状态，无需行锁
    x = db.get(MergeDrill, ident)
    if not x:
        raise HTTPException(404, '合并演练不存在')
    eng = _engine(x)
    try:
        if snapshot_id is not None:
            return eng.snapshot_read(snapshot_id, key)
        return eng.point_read(key)
    except KeyError as exc:
        raise HTTPException(404, str(exc))


def start_drill(db, ident, data):
    x = _locked(db, ident)
    eng = _engine(x)
    try:
        eng.start_drill(data.name, data.selected_table_ids,
                        mode=data.mode, drop_tombstones=data.drop_tombstones)
    except ValueError as exc:
        raise HTTPException(422, str(exc))
    x.state = eng.s
    _sync_status(x, eng)
    db.commit()
    queue.rpush(JOB_KEY, ident)
    return serialize(x)


def prepare_job(db, ident):
    """worker 调用：行锁内预构。失败落 last_error 且演练标记 failed，不改变层级。"""
    x = _locked(db, ident)
    eng = _engine(x)
    if eng.s['drill'] is None or eng.s['drill']['status'] in ('effective',):
        return None
    try:
        eng.recover()
        eng.prepare_drill()
        x.last_error = None
    except ValueError as exc:
        if eng.s['drill'] is not None:
            eng.s['drill']['status'] = 'failed'
            eng.s['drill']['phase'] = 'prepare_failed'
            eng._archive_drill()
        x.last_error = str(exc)[:500]
    x.state = eng.s
    _sync_status(x, eng)
    db.commit()
    return x.status


def apply_drill(db, ident):
    x = _locked(db, ident)
    eng = _engine(x)
    try:
        eng.apply_drill()
        x.last_error = None
    except ValueError as exc:
        x.last_error = str(exc)[:500]
        raise HTTPException(409, str(exc))
    finally:
        x.state = eng.s
        _sync_status(x, eng)
        db.commit()
    return serialize(x)


def gc_evaluate(db, ident):
    x = db.get(MergeDrill, ident)
    if not x:
        raise HTTPException(404, '合并演练不存在')
    return MergeEngine(x.state).gc_evaluate()


def recover_pending(db):
    """worker 启动/空轮询：恢复中断演练并保证待预构任务都在队列里。

    - preparing（预构中途崩溃）：引擎重置为 pending 并落库；
    - 普通 pending（仅排队，可能 Redis 队列丢失）：重新入队，不改动状态。
    ready/effective 不受影响。
    """
    rows = db.scalars(select(MergeDrill)).all()
    requeue = []
    for x in rows:
        eng = _engine(x)
        d = eng.recover()
        if d is not None:                 # 真正从 preparing 恢复
            x.state = eng.s
            x.status = 'pending'
        drill = eng.s['drill']
        if drill and drill['status'] == 'pending':
            requeue.append(x.id)
    db.commit()
    for ident in requeue:
        queue.rpush(JOB_KEY, ident)
    return requeue
