# 边缘遥测 LSM 合并工坊

北岭边缘计算小组的有序键值写入与合并演练：记录写入批次、配置层级策略、后台执行刷盘与合并，用最后写入字典 Oracle 核查读结果，防止墓碑误删导致旧值复活。

**这是 Python 确定性教学 LSM 模型。MySQL 8.0 仅保存业务元数据，不是对 MySQL/InnoDB 存储内核的实现或性能测试。** 页面与导出结果均保留此边界。

## 场景与模块

北岭边缘计算小组有 7 人，负责低内存遥测站点的存储方案验证。过去在电子表格记录写入序列、手工画 SSTable、用本地脚本核对合并结果，三个痛点是配置与操作序列混在一起、删除键的历史难复查、读写放大缺少一致口径。上游为站点模拟写入序列；本站替代输入归档、配置组合与可重现演练；下游是边缘存储评审人与值班交接。

- 写入批次：有序 put/delete，支持重复键、墓碑、完整操作预览、输入校验。
- 层级配置：memtable 不同键容量、SSTable 合并触发数、层级上限、最终完整合并开关。
- 合并队列：创建试验时持久化输入及配置快照，Redis 分发，独立 worker 执行并记录状态。
- 读放大证据：显示合并前后层级、读写放大、真实存活键、Oracle 摘要、刷盘/合并轨迹、JSON 导出。
- 合并演练（可持续写入）：在线引擎把状态整存 MySQL，支持持续 put/delete、创建/释放读取快照与按键查询当时值、自选部分 SSTable 发起 tiered/full 合并；影子区预构后整批切换，中断可恢复、重试不重复计写放大，并逐表给出旧值/墓碑的保留或清理依据。
- 管理员审计：记录写操作账号、路由与 HTTP 结果，包含权限拒绝。

| 角色 | 页面与权限 | 典型动作 |
|---|---|---|
| admin 管理员 | 全业务页面与审计 | 登记批次/配置、执行、导出、查看审计 |
| operator 操作员 | 四业务页面，不可看管理员审计 | 新建输入与配置、提交 worker、检查与下载结果 |
| viewer 观察员 | 四业务页面只读与导出 | 阅读层级和 Oracle 证据，写请求在 API 层403 |

## 启动与账号

```bash
cp .env.example .env
docker compose config --quiet
docker compose up -d --build
# 默认启动自动执行；手动重跑不覆盖已存在的输入、配置、账号
docker compose exec -T backend python -m scripts.seed_demo
```

[前端入口](http://localhost:18245) · [后端接口文档](http://localhost:19245/docs)，数据库映射端口17745。

三个本地演示账号是 `admin@beiling.test` / `operator@beiling.test` / `viewer@beiling.test`，统一密码 `EdgeStore!17`。`.env.example` 提供固定本地数据库与JWT演示凭据，上线前替换。

```bash
# 只清理本项目与其演示卷
docker compose down -v --remove-orphans
```

## 09:00—10:00 的工作剧本

1. 09:00 operator 登录，打开“北岭站点-早班写入”：12条操作，8个不同键。
2. 09:05 查看 sensor-01 的18.2→18.8→19.0覆盖记录，sensor-02和sensor-03有删除墓碑。
3. 09:10 新建 `shift-20261117` 输入，或复用“旧值复活回归集”。
4. 09:15 选择“小内存-完整合并”：memtable键容量2，表触发数2，3层，最后完整合并。
5. 09:20 创建试验并提交worker，在队列刷新至“Oracle一致”。
6. 09:25 打开读放大证据，确认早班输入最终6个存活键，sensor-02/03未复活。
7. 09:30 下载 `lsm-trial-<id>.json`，包含操作/配置快照、层级、轨迹与校验和。
8. 09:40 viewer复核，创建试验被403拒绝；10:00 admin检查操作审计。

Seed另含“旧值复活回归集”和“全部删除边界集”；配置另含“保留层级观测”和“宽缓冲-完整合并”。账户、批次、配置以自然键查缺插入，重复seed不改变已存在字段。

## 在线合并演练引擎（merge 页面）

整批 `simulate` 只能事后核对结果；在线引擎 `services/merge_engine.py` 把全部存储状态保存在可 JSON 序列化的 `state` 中，由 `merge_drills` 表整存，可随时中断恢复。它是确定性教学模型，不代表任何真实存储引擎。

- 持续写入：put/delete 分配单调序号，memtable 同键覆盖，达到 `memtable_limit` 刷成不可变 L0 SSTable；在线引擎不自动做层级合并，L0 达阈值仅提示，由用户自选表发起演练。
- 读取快照：创建时复制 memtable 并记录当前全部 SSTable 的 id 引用（上限 50 个）。快照点查只走快照视图，后续覆盖、删除、合并不影响它，快照永远读创建时刻的值。
- 自选表合并：同层选择为 `tiered`（结果写下一层，始终保留墓碑）；跨层必须选中当前全部表才允许 `full`（结果写最深层）。`full` 仅当**没有存续快照**时才清理墓碑；有快照时墓碑保留。
- 两阶段提交：演练状态 `pending → preparing → ready → effective/failed`。worker 只在影子区预构（`prepare`），不产生任何写放大；页面显式 `apply` 时先在临时结构上完成 Oracle 与快照隔离全部校验，通过后才一次性落地层级、归档与计数。任何校验失败都标记 failed 且不留半切换状态。
- 中断恢复：worker 启动和空轮询把卡在 pending/preparing 的演练恢复为 pending 重新入队；ready 已持久化，重启不回退。写放大只在 apply 落地一次，重试不重复。
- GC 依据：被合并的旧表若仍被存续快照引用则移入 `archive` 支撑快照读，释放后才清理；无快照引用的 full 表在快照释放时重写移除墓碑，重写条目计入独立的 `gc_rewrite_writes`，**不计入合并写放大**。`gc_evaluate` 对每张表输出 retain/reclaimable 判定与原因，页面与导出均可核对。
- 并发：服务层每次变更都在 `SELECT ... FOR UPDATE` 行锁内串行化；合并期间可继续写入与创建快照，新写入只进 memtable/新 L0，不参与也不会被进行中的演练吞掉。同一演练实例同时只允许一个未完成演练。
- 校验与口径：apply 后全部已知键点查必须与最后写入 Oracle 一致（已删键不复活），存续快照视图校验和不变；写放大=(刷盘写出+合并写出)/用户操作数；读放大沿用固定点查集合（全部输入键+一个不存在探针）。

`merge_drills` 为 schema v2 新增表，`create_all` 幂等建表，不改动 v1 的 trials/datasets/configs；原合并队列、读放大证据页与 `lsm-trial-<id>.json` 下载保持可用，演练导出为独立的 `lsm-merge-drill-<id>.json`。

## 核心算法与指标口径

输入为有序操作数组和参数配置：

- 操作数1–2000；键长1–64；put的value必须是1个不超过200字符的字符串（允许空字符串）；delete的value忽略。
- memtable_limit范围2–64，按不同键数量计容量；l0_trigger范围2–8；max_levels范围2–6。
- 每个用户操作分配单调序号。memtable同键覆盖；达到容量后按键排序刷成L0 SSTable。末尾不足容量也刷盘。
- 某层表数达到触发值时，合并该层所有表到下一层；相同键按最高序号保留记录。最深层在原层合并。**层级合并始终保留墓碑**，不会提早清理。
- 若打开最终完整合并，收集全部层级所有表，按序号合并完整历史后才移除墓碑，存放在最深层。不开启时保留中间层级与墓碑。
- 点查遍历范围覆盖键的SSTable，在候选记录中选择最高序号；带墓碑则不返回。固定探测集合为全部输入键加一个不存在的探针键。没有Bloom过滤器模型。
- 读放大 = 固定点查集合的范围命中SSTable读取总数 / 探测键数；合并前后使用相同集合。写放大 = flush/各次compaction真实输出条目总数 / 用户操作数，**不是物理字节或磁盘I/O**。
- Python另从原始操作顺序构造最后写入字典Oracle；合并前、合并后均必须与Oracle一致，否则试验failed。结果与Oracle各自对有序JSON求SHA-256；同输入/配置结果确定，无随机时钟项。

输出包括真实计算的层级表数/键范围/墓碑数、每步刷盘合并轨迹、输入键数、存活键、写出条目、读写放大、Oracle及实际键值、摘要。空输入和超限在提交时422，引用不存在404，重名409，排队/运行总数超过20时429；worker失败保存error和failed状态。

## 技术栈与代码组织

| 服务 | 实现与作用 |
|---|---|
| frontend | 原生HTML/CSS/JS ES模块，Nginx1.27，土橙材料样本册视觉 |
| backend | Python3.11、FastAPI、SQLAlchemy、PyMySQL、JWT与PBKDF2密码 |
| worker | 独立Python进程，Redis BLPOP，LSM演算，数据库恢复队列，心跳健康检查 |
| db | MySQL8.0，持久化3个业务实体、用户与审计 |
| redis | Redis7，异步队列与持久卷 |

`backend/src/`分为config/models/schemas/services/controllers/routes/middlewares/utils/constants；`backend/scripts/seed_demo.py`种子与初始化；`backend/migrations/README.md`为schema v1说明。3实体dataset/config/trial各自贯穿model/schema/service/route、前端api/store/page。试验结果保存在Trial JSON字段中，详情页为独立业务页面。

`frontend/src/`包含api/stores/types/components/common/hooks/pages/router/utils/constants，表格/状态/页头跨页共享，HTML转义与反馈工具复用。角色/任务状态/操作类型枚举在`backend/src/constants/enums.py`和`frontend/src/constants/enums.js`。模型默认任务状态使用后端枚举，前端状态组件使用前端枚举。

Docker恰好5服务，全部healthcheck；backend依赖db/redis healthy，worker还等待backend完成seed；frontend等待backend healthy。命名卷保存MySQL、Redis及worker证据。Nginx `/api/`剥前缀转发后端。

环境变量：COMPOSE_PROJECT_NAME、FRONTEND_PORT、BACKEND_PORT、DB_PORT、MYSQL_PASSWORD、MYSQL_ROOT_PASSWORD、JWT_SECRET、SEED_DEMO；容器连接还配置DATABASE_URL/REDIS_URL。端口可在.env整体调整避免冲突。

本地开发推荐修改代码后`docker compose up -d --build`。也可用虚拟环境安装backend/requirements.txt，启动Compose db/redis，设置可达DATABASE_URL/REDIS_URL/JWT_SECRET，进入backend执行`uvicorn src.main:app --reload`；worker须有可写`/artifacts`并执行`python -m src.worker`。

## 已知边界

- 存储过程为内存教学模型，忽略磁盘块、压缩、Bloom过滤器、WAL持久化、并行compaction与真实故障恢复。MySQL负责保存网页业务输入和输出；在线演练的引擎状态也整存在 MySQL JSON 字段，而非真实落盘的 SSTable。
- 前端创建独立批次/配置，不提供原记录编辑删除；新版本新建记录，整批试验总是固定输入快照。
- 整批全历史合并允许去掉墓碑；在线演练额外模拟了读取快照与快照隔离（memtable 副本 + 不可变 SSTable 引用），但仍是单机教学模型，未模拟跨副本/MVCC 事务历史，不能将该规则直接用于生产数据库。
- 在线演练的 GC 重写与整批切换是确定性的内存操作，不模拟真实磁盘空间回收与 I/O；行锁仅在单 MySQL 单 worker 模型下提供串行化，不包含分布式锁。
- 单worker与按钮刷新；演练 pending/preparing 提供两秒轮询，不提供多worker、SSE。
- Web双字体来自Google Fonts，离线回退到serif/monospace。

MIT License，见LICENSE。实际测试与截图位于批次的`test-artifacts/gb-4106/`，源码内不保存测试证据。
