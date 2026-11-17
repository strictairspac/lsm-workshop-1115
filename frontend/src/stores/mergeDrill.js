import{list}from'../api/mergeDrill.js';
export const mergeDrills={items:[],async load(){this.items=await list();return this.items}};
