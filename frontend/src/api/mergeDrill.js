import{api,post}from'./client.js';
const base='/merge-drills';
export const list=()=>api(base);
export const detail=id=>api(base+'/'+id);
export const create=data=>post(base,data);
export const writeOne=(id,data)=>post(base+'/'+id+'/writes',data);
export const bulkWrite=(id,entries)=>post(base+'/'+id+'/bulk-writes',{entries});
export const createSnapshot=id=>post(base+'/'+id+'/snapshots',{});
export const releaseSnapshot=(id,sid)=>api(base+'/'+id+'/snapshots/'+sid,{method:'DELETE'});
export const readKey=(id,key,snapshot_id)=>post(base+'/'+id+'/read',{key,snapshot_id:snapshot_id??null});
export const startDrill=(id,data)=>post(base+'/'+id+'/start',data);
export const applyDrill=id=>post(base+'/'+id+'/apply',{});
export const gcEvaluate=id=>api(base+'/'+id+'/gc');
export async function download(id){const full=await api(base+'/'+id+'/export');const url=URL.createObjectURL(new Blob([JSON.stringify(full,null,2)],{type:'application/json'}));const a=document.createElement('a');a.href=url;a.download='lsm-merge-drill-'+id+'.json';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000)}
