ROLES=('admin','operator','viewer')
STATUSES=('queued','running','passed','failed')
OPERATIONS=('put','delete')
# 在线合并演练状态：待合并 -> 预构中 -> 待切换 -> 已生效 / 失败
DRILL_STATUSES=('pending','preparing','ready','effective','failed')
MERGE_MODES=('auto','tiered','full')
