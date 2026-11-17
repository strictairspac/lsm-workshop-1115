import{mergeDrills}from'../stores/mergeDrill.js';
import{configs}from'../stores/config.js';
import{detail,create,writeOne,bulkWrite,createSnapshot,releaseSnapshot,readKey,startDrill,applyDrill,download}from'../api/mergeDrill.js';
import{header}from'../components/common/header.js';
import{table}from'../components/common/table.js';
import{badge}from'../components/common/badge.js';
import{esc,json}from'../utils/html.js';
import{submit,notify}from'../hooks/feedback.js';
import{session}from'../stores/session.js';
import{MERGE_MODES}from'../constants/enums.js';

const writable=()=>session.get().role!=='viewer';

function layersView(d){
  return '<div class="layers">'+d.levels.map(lv=>'<div class="layer"><span class="mono">L'+lv.level+'</span><div class="slabs">'+
    (lv.tables.length?lv.tables.map(t=>'<label class="slab pick"><input type="checkbox" class="pick-table" value="'+t.id+'" '+(d.drill?'disabled':'')+'><span>#'+t.id+' · '+esc(t.origin)+(t.full_compaction?' · full':'')+'<br>'+t.entries+' 条目 / '+t.tombstones+' 墓碑<br>'+esc(t.min_key)+' … '+esc(t.max_key)+'</span></label>').join(''):'<span class="muted">空层</span>')+
    '</div></div>').join('')+'</div>';
}

function gcView(d){
  return table(['表','层/来源','条目','墓碑','归档','快照引用','判定','依据'],d.gc.map(g=>[
    '#'+g.table_id,'L'+g.level+' / '+esc(g.origin),g.entries,g.tombstones,g.archived?'是':'否',
    g.snapshot_refs.length?g.snapshot_refs.map(x=>'快照'+x).join(', '):'—',
    '<span class="badge '+(g.decision==='reclaimable'?'different':'passed')+'">'+(g.decision==='reclaimable'?'可清理':'保留')+'</span>',
    esc(g.reason)]));
}

function drillPanel(d){
  const drill=d.drill;
  if(!drill)return '<p class="helper">当前没有进行中的演练。勾选上方 SSTable 后可发起合并。</p>';
  return '<div class="modelnote">演练 <strong>'+esc(drill.name)+'</strong> · '+esc(drill.kind)+' · 目标 L'+drill.target_level+
    ' · 清墓碑：'+(drill.drop_tombstones?'是':'否（有存续快照或非 full）')+'<br>'+badge(drill.status)+' <span class="muted">'+esc(drill.phase)+'</span>'+
    (drill.output_entries!=null?'<br>预构输出 '+drill.output_entries+' 条目，等待整批切换':'')+
    (drill.status==='ready'&&writable()?'<div class="actions"><button class="primary" id="apply-drill">整批切换生效 →</button></div>':'')+
    '</div>';
}

export async function mergePage(el){
  const cs=await configs.load();
  await mergeDrills.load();
  const id=Number(location.hash.split('/')[1])||mergeDrills.items[0]?.id;
  const d=id?await detail(id):null;
  const listRows=mergeDrills.items;
  el.innerHTML=header('05','可持续写入的合并演练','写入持续落 memtable/SSTable，快照持有旧历史；自选部分表发起演练，先在影子区预构再整批切换。旧值与墓碑仅在无存续快照且未被选中需要时才清理，已删键不复活。')+
  '<div class="split"><section class="panel"><h2>演练实例</h2>'+
  table(['演练','配置','状态','操作'],listRows.map(x=>[esc(x.name)+'<br><small>#'+x.id+'</small>','#'+x.config_id,badge(x.status),'<a href="#merge/'+x.id+'">打开 →</a>']))+
  (writable()?'<form id="create-form" style="margin-top:20px"><label>新建演练名称</label><input name="name" value="drill-'+Date.now().toString().slice(-6)+'" required maxlength="120"><label>存储配置</label><select name="config_id">'+cs.map(c=>'<option value="'+c.id+'">'+esc(c.name)+'</option>').join('')+'</select><div class="actions"><button class="primary" type="submit">创建演练</button></div></form>':'')+
  '</section><aside class="panel"><h2>口径与边界</h2><p class="helper">教学在线 LSM 模型：Python 内存算法，MySQL 整存引擎状态，非真实存储引擎。<br><br>· tiered 同层合并始终保留墓碑；full 必须选全部表，且无存续快照才清墓碑。<br>· 快照释放前，被合并的旧表移入归档支撑快照读。<br>· 预构不计写放大，整批切换只计一次，中断重试不重复。</p></aside></div>';

  if(d){
    el.innerHTML+='<section class="panel"><div class="actions" style="margin-top:0"><h2 style="margin:0">'+esc(d.name)+' #'+d.id+'</h2>'+badge(d.status)+
      '<button id="refresh-drill">刷新状态</button>'+
      (writable()?'<button id="download-drill">下载完整 JSON ↓</button>':'')+'</div>'+
      '<div class="metrics">'+
      '<div><small>用户操作 / 存活键</small><strong>'+d.summary.user_ops+' / '+d.summary.live_keys+'</strong></div>'+
      '<div><small>写放大(刷盘+合并)</small><strong>'+d.summary.write_amplification+' ×</strong></div>'+
      '<div><small>读放大 / 探测键</small><strong>'+d.summary.read_amplification+' / '+d.summary.probe_count+'</strong></div>'+
      '<div><small>快照 / 归档表</small><strong>'+d.snapshots.length+' / '+d.archive.length+'</strong></div>'+
      '<div><small>GC 重写条目(不计合并)</small><strong>'+d.summary.gc_rewrite_writes+'</strong></div>'+
      '<div><small>Oracle 校验</small><strong>'+(d.summary.actual_matches_oracle?'一致':'不一致')+'</strong></div>'+
      '</div>'+
      (writable()?'<form id="write-form" class="inline-form"><label>持续写入</label><input name="key" placeholder="键 1-64 字" required maxlength="64"><select name="op"><option value="put">put</option><option value="delete">delete</option></select><input name="value" placeholder="value(put 必填)"><button type="submit">写入一笔</button></form>':'')+
      (writable()?'<form id="bulk-form"><label>批量写入 · JSON 数组</label><textarea name="entries" rows="4">[\n  {"op":"put","key":"sensor-x","value":"1"}\n]</textarea><div class="actions"><button type="submit">批量写入</button></div></form>':'')+
      '<h2 style="margin-top:25px">层级与 SSTable（勾选参与合并）</h2>'+layersView(d)+
      (writable()?'<form id="drill-form" class="inline-form"><label>演练名称</label><input name="name" value="merge-'+Date.now().toString().slice(-6)+'" required maxlength="120"><label>模式</label><select name="mode">'+Object.entries(MERGE_MODES).map(([k,v])=>'<option value="'+k+'">'+esc(v)+'</option>').join('')+'</select><label>清墓碑(full 且无快照才生效)</label><select name="drop_tombstones"><option value="true">是</option><option value="false">否</option></select><button class="primary" type="submit" '+(d.drill?'disabled':'')+'>发起合并演练</button></form>':'')+
      drillPanel(d)+
      '<h2 style="margin-top:25px">读取快照</h2>'+
      (d.snapshots.length?table(['快照','创建序号','表引用','mem 键','键数','校验和','操作'],d.snapshots.map(s=>['#'+s.id,s.created_seq,s.table_refs,s.mem_keys,s.keys,'<small class="mono">'+esc(s.checksum.slice(0,16))+'…</small>',writable()?'<button class="release-snap" data-sid="'+s.id+'">释放</button>':'—'])):'<div class="empty">尚无快照。</div>')+
      (writable()?'<div class="actions"><button id="make-snapshot">创建读取快照</button></div>':'')+
      '<form id="read-form" class="inline-form"><label>按键查询</label><input name="key" placeholder="要查询的键" required><select name="snapshot_id"><option value="">当前视图</option>'+d.snapshots.map(s=>'<option value="'+s.id+'">快照 #'+s.id+'</option>').join('')+'</select><button type="submit">查询当时值</button></form>'+
      '<div id="read-result"></div>'+
      '<h2 style="margin-top:25px">保留 / 清理依据</h2>'+gcView(d)+
      '<h2 style="margin-top:25px">时间线（近 30 步）</h2>'+
      table(['步骤','事件','层/表','层级表数'],d.timeline.map(t=>[t.step,esc(t.event),(t.level??'—')+(t.table_id?'/表'+t.table_id:''),JSON.stringify(t.level_table_counts)]))+
      '<h2 style="margin-top:25px">GC 日志（近 30 条）</h2>'+
      table(['#','动作','表','依据'],d.gc_log.map(g=>[g.step,esc(g.action),'#'+g.table_id,esc(g.reason)]))+
      '</section>';
  }
  bindEvents(el,id);
  // worker 异步预构：演练处于 pending/preparing 时自动轮询，状态稳定后停止
  if(d&&d.drill&&['pending','preparing'].includes(d.drill.status)){
    clearTimeout(window.mergePoll);
    window.mergePoll=setTimeout(()=>mergePage(el),2000);
  }
}

function bindEvents(el,id){
  const refresh=()=>{clearTimeout(window.mergePoll);mergePage(el)};
  const cf=el.querySelector('#create-form');
  if(cf)cf.onsubmit=e=>{e.preventDefault();submit(e.target,async()=>{const d=Object.fromEntries(new FormData(e.target));d.config_id=Number(d.config_id);const x=await create(d);notify('演练已创建');location.hash='merge/'+x.id;await refresh()})};
  const wf=el.querySelector('#write-form');
  if(wf)wf.onsubmit=e=>{e.preventDefault();submit(e.target,async()=>{const d=Object.fromEntries(new FormData(e.target));if(d.op==='put'&&!d.value)throw Error('put 必须提供 value');await writeOne(id,d);notify('写入已持久化');await refresh()})};
  const bf=el.querySelector('#bulk-form');
  if(bf)bf.onsubmit=e=>{e.preventDefault();submit(e.target,async()=>{let entries;try{entries=JSON.parse(new FormData(e.target).get('entries'))}catch{throw Error('JSON 格式错误')}await bulkWrite(id,entries);notify('批量写入已持久化');await refresh()})};
  const df=el.querySelector('#drill-form');
  if(df)df.onsubmit=e=>{e.preventDefault();submit(e.target,async()=>{const ids=[...el.querySelectorAll('.pick-table:checked')].map(x=>Number(x.value));if(!ids.length)throw Error('请至少勾选一张 SSTable');const f=Object.fromEntries(new FormData(e.target));await startDrill(id,{name:f.name,selected_table_ids:ids,mode:f.mode,drop_tombstones:f.drop_tombstones==='true'});notify('演练已入队，worker 正在影子区预构');setTimeout(()=>refresh(),600)})};
  const applyBtn=el.querySelector('#apply-drill');
  if(applyBtn)applyBtn.onclick=()=>applyDrill(id).then(()=>{notify('已整批切换生效');refresh()}).catch(e=>notify(e.message));
  const snapBtn=el.querySelector('#make-snapshot');
  if(snapBtn)snapBtn.onclick=()=>createSnapshot(id).then(()=>{notify('读取快照已创建');refresh()}).catch(e=>notify(e.message));
  el.querySelectorAll('.release-snap').forEach(b=>b.onclick=()=>releaseSnapshot(id,Number(b.dataset.sid)).then(()=>{notify('快照已释放，已重新评估 GC');refresh()}).catch(e=>notify(e.message)));
  const rf=el.querySelector('#read-form');
  if(rf)rf.onsubmit=e=>{e.preventDefault();const f=Object.fromEntries(new FormData(e.target));readKey(id,f.key,f.snapshot_id?Number(f.snapshot_id):null).then(r=>{el.querySelector('#read-result').innerHTML='<pre class="json">'+json(r)+'</pre>'}).catch(e=>notify(e.message))};
  const dl=el.querySelector('#download-drill');
  if(dl)dl.onclick=()=>download(id).catch(e=>notify(e.message));
  const rb=el.querySelector('#refresh-drill');
  if(rb)rb.onclick=()=>refresh();
}
