# 分步续接加工：公开新增契约

已有能力、数据格式、SDK1精确语义、目录和任务接口见 CONTRACT.md。
本题只给 SDK1 任务增加分步执行与持久续接，SDK2 和离线 v2 格式不接入。
保留原接口及原测试文件，sdk1.py 保留为整批语义参照。不要求优化内存、
检查点大小或序列化成本；只需单工作者、串行调用，不做进程终止恢复或调度。

## 分步运行时

新增 resumable.start(plan, parameters) 与 resumable.advance(checkpoint, budget)。
start 按 sdk1.validate/bind 的原规则校验、复制输入，错误沿用 WarehouseError，
但不进入任何表达式求值。它返回仅含 JSON 数据的初始检查点。调用者随后
改变原计划或参数不能影响该检查点。内部格式由实现选择，不依赖生成器、
进程内注册表、对象身份或数据库连接；JSON 往返乃至新进程中均可恢复。
只要求接受 start/advance 自己产生的合法检查点，不要求接纳手写坏检查点。

advance 返回恰含以下字段的字典：
{"checkpoint": 新检查点, "state": "queued"或"done"或"failed",
 "used": 本次进入节点数, "output": 结果或null, "error": 错误或null}。
queued 时 output/error 均为 null；done 的 output 为完整业务值、error 为 null；
failed 的 output 为 null、error 为 {"code": 字符串, "message": 字符串}。
成功 null 由 state=done 区分。合法推进允许复用或修改传入检查点，调用者以后
以返回的 checkpoint 为准。终态检查点再次推进返回原终态和结果/错误，used=0。

budget 必须为非布尔正整数；否则抛 ValueError，不能修改检查点或持久状态，
也不能进入任何节点，包括针对终态任务的非法预算调用。
一次工作单位是进入一个SDK1表达式节点求值；同一正文对不同元素的求值分别计数，恢复已进入节点不再次计数。
每步持续推进直到用完 budget 或终态；每步最多进入 budget 个节点。
预算耗尽后允许完成已进入节点的返回、类型检查和错误传播，不允许进入下一节点。
没有剩余表达式可进入时必须完成传播并返回终态，不能多要求一次零工作推进。
结构校验、复制业务值、序列化和从子表达式返回均不计节点。
不得从头求值后丢弃前缀，或重新进入任何先前成功提交的已进入节点。

完整语义及首次错误的code/message必须与原sdk1.execute一致，包括add左右顺序、if未选分支不求值、map完整源先于正文、fold完整源先于initial、null源跳过无用计算以及同名局部遮蔽。
例如 add 左边为 []、右边为未绑定变量 right，先报 unbound/right 而非 type；
fold 源内部出错时，不能先求 initial。暂停允许发生在源内部、正文内部、尚未完成的操作数及多层同名绑定内部，不能只支持外层数组元素边界。

为核验真实工作，每次动态进入表达式前，必须调用 progress_probe.entered(node)，
node 是本次所求的完整 SDK1 表达式字典（可为等值副本）。调用时扣本次一个节点；
恢复该节点及检查点扫描、结构校验不得调用此钩子。钩子异常作为非业务异常传播。
该钩子仅观察，不提供执行、续栈或计费实现；无观察者时不保存轨迹。
原 sdk1.execute 不接此钩子，夹具独立包装其递归入口取得参照轨迹。

## 持久任务

新增 Jobs(db).step(id, budget)，返回恰为 {"state": 状态, "used": 本次节点数,
"total": 此任务累计已确认节点数}。状态仍只有 queued/done/failed。
每次调用将新检查点、累计total以及可能产生的结果或业务错误在同一SQLite事务中提交，非业务异常整次回滚。
使用既有 Database.transaction 提交边界，使 progress_probe.fail_before_commit(db)
可以注入提交前异常。不要求公开检查点的 SQL 表名、列名或增设存储版本；
允许增加辅助表/列，继续打开既有 v1 库，不迁移 SDK2。

任意成功step后关闭并重开，下一次必须从检查点继续，不能重新进入已提交完成或已进入的表达式节点；提交前异常后重试允许重做该次未提交工作。
任务固定的修订和参数快照保持不变；暂停期间更新 head 不能改变旧任务。
result 在 queued 时仍报 pending；部分数组与fold累加值不得作为结果公开。
终态 step 返回 used=0 且 total 不变；原有历史终态任务初始 total=0，
不能为了补计数重新执行它们。旧 queued 任务首次 step 从未执行状态开始。
run(id) 仍运行到 done/failed，须接续已暂停任务而非重新开始；本次实际执行的
节点也加入 total。run 继续遵守原业务错误和非业务异常的终态/回滚约定。
status/result 的返回结构、复制语义和 not_found 等原接口行为保持。

## 完整小例

输入仍用 samples 的 AST 构造函数：
plan = P([], A(C(1), C(2)))，参数为 {}，初始空任务。
连续三次 step(id,1) 依次返回：
{"state":"queued","used":1,"total":1}
{"state":"queued","used":1,"total":2}
{"state":"done","used":1,"total":3}
随后 result(id)=3，再次 step(id,1) 返回 done/0/3。
三个节点依次为 add、左 const、右 const。若第三次在提交前抛非业务异常，
重开后仍 pending、total=2；下一次 step 只重新进入右 const，得到 done/1/3。

samples.nested() 的参数 {"rows":[[10,20],[],[null,10]],"base":7}
结果仍是 [[37,57],[],[null,37]]，动态进入共 76 个表达式。
以预算 7 暂停并重开后，run 只进入剩下的 69 个表达式；head 更新不能改变结果。

## 运行与交付

当前完整旧回归和旧演示：
python3.11 -B -m unittest discover -s tests -v
python3.11 -B cli.py demo
非答案夹具自检（仅内存库）：python3.11 -B progress_probe.py

resume_checks.py 已提供可运行检查及真实磁盘关闭重开演示；它未实现待开发能力，
当前执行其 verify/demo 会因新增入口尚未实现而失败。完成后执行：
python3.11 -B resume_checks.py verify
python3.11 -B resume_checks.py demo
verify 先原样运行全部旧测试，再核对分步结果、错误与进入轨迹，并验证旧库、
固定修订、关闭重开和提交前异常回滚；demo 展示 nested 暂停及重开续接的真实结果。
这些样例不是全部边界，交付还须补充必要回归，核对两种入口的非法预算、
业务失败提交回滚，以及嵌套源、初值、正文和操作数之间的暂停。

沿用 .pointwise-runtime.json 的版本1 CLI格式，setup 为准备命令的 argv 列表，
verify/demo 各为单个 argv；完成后让 verify 指向 resume_checks.py verify，
demo 指向 resume_checks.py demo，保留旧 cli.py demo 可运行。
提交实现和必要状态不变量说明，并说明运行时状态如何交给任务事务确认。
