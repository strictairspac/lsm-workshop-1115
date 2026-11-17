export const ROLES={admin:'管理员',operator:'操作员',viewer:'观察员'};
export const STATUSES={queued:'排队中',running:'合并中',passed:'Oracle 一致',failed:'执行失败'};
export const OPERATIONS={put:'写入',delete:'墓碑删除'};
export const DRILL_STATUSES={idle:'空闲可写入',pending:'待合并',preparing:'预构中',ready:'待切换',effective:'已生效',failed:'已中断'};
export const MERGE_MODES={auto:'自动判定',tiered:'同层合并(保留墓碑)',full:'完整历史(可清墓碑)'};
