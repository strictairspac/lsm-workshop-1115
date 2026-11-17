import{STATUSES,DRILL_STATUSES}from'../../constants/enums.js';
import{esc}from'../../utils/html.js';
export const badge=(s,map)=>{
  const labels=map||{...STATUSES,...DRILL_STATUSES};
  return '<span class="badge '+esc(s)+'">'+esc(labels[s]||s)+'</span>';
};
