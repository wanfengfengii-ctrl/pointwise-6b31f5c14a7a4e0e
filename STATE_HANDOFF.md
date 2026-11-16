# 分步续接状态交接

本说明对应 RESUMABLE.md 新增能力：`resumable.start/advance` 与 `Jobs.step`。
原有 SDK1 整批语义（sdk1.py）、目录/队列公开接口、SQLite v1 历史库和 SDK2
离线端口均未改动；run 现在改为在既有事务内驱动同一续接栈。

## 运行时状态不变量

`resumable.start(plan, parameters)` 只做 `sdk1.validate` 与 `sdk1.bind`，
不进入任何表达式、不调用 `progress_probe.entered`，并深拷贝计划与参数。
检查点只含 JSON 数据：

```
{"version":1, "plan":<验证过的计划副本>,
 "stack":[帧...], "pending":null|{"value":业务值},
 "state":"queued"|"done"|"failed", "output":..., "error":...}
```

帧为 `{"node":表达式,"environment":{名字:业务值},"entered":bool,
"phase":..., "left"/"source"/"index"/"result"/"acc"/"branch"/"pending child"}`。
环境是普通字典的**逐层快照**，新增绑定用新字典，因此同名遮蔽只影响内层，
恢复时无需重放外层求值。

不变量：

1. **进入即计数**：`entered` 在首次进入帧时置真并调用一次
   `progress_probe.entered(node)`；`used` 就是本步这样的置真次数。
   恢复已有帧、扫描检查点、结构校验、从子表达式返回和序列化都不计数。
2. **已确认节点不重放**：帧保存在 `stack` 中，已求出的操作数/源/累加值
   存为字段，预算耗尽时下一次 `advance` 从同一帧相位继续，绝不重新进入。
3. **返回顺序即契约**：每个 op 的 `phase` 严格按 sdk1.evaluate 的顺序推进
   （add 先左后右再 plus；if 先 test 再选中分支；map 先完整源；fold 先源、
   后 initial、再逐项 body），业务错误在首次出现处抛出并冻结为 failed。
4. **预算**：非布尔正整数，否则 `ValueError`，在读取/复制检查点之前抛出，
   因此终态任务与未进入任何节点的暂停任务都不受影响。预算用尽后仍允许
   完成已进入帧的返回、类型检查与错误传播；栈空且无待交付值即得终态。
5. **原子性**：终态与 queued 检查点、累计 total、output/error 均只经
   `Database.transaction()` 的同一提交边界发布；业务错误同样提交为 failed，
   任何其他异常（含注入的 `CommitFailure`、钩子异常）整次回滚。

## 交给任务事务确认

`Jobs.step(id, budget)` 在 `BEGIN IMMEDIATE` 中：读固定修订的计划快照、
取（或首次建立）检查点、调用 `resumable.advance`、把返回的新检查点与
`total += used` 写回辅助表 `job_checkpoints_v1(job_id, checkpoint, total)`，
并在同一事务内按终态更新 `jobs_v1` 的 `state/output/error`。
检查点先于业务值可见：queued 时 jobs_v1 仍为原样，`result` 报 pending，
部分数组与 fold 累加值只存在于检查点内部，不对外公开。

`job_checkpoints_v1` 采用 `CREATE TABLE IF NOT EXISTS`，因此仍可直接打开
既有 v1 库（含 samples.create_frozen 造出的历史库），不改 `meta.version`。
历史终态任务没有检查点行，`step` 返回 `used=0`、`total=0`，绝不补算。
历史 queued 任务首次 step 由 jobs_v1 中已固定修订和参数快照新建检查点。

`run(id)` 不再调用 `sdk1.execute`，而是在同一事务内循环 `resumable.advance`，
把本次执行节点累加进 total；它接续已暂停任务、遵守相同的业务错误/回滚约定。
`status/result/not_found` 的返回结构和复制语义保持原样。

## 运行清单

`.pointwise-runtime.json` 版本仍为 1，`setup` 为空列表：

* `verify` → `resume_checks.py verify`（先原样跑完旧测试，再核对分步结果、
  进入轨迹、旧库、固定修订、关闭重开与提交前异常回滚）
* `demo` → `resume_checks.py demo`（nested 以预算 7 暂停、关库、更新 head、
  重开续接 69 个条目）

旧入口 `python3.11 -B cli.py demo`、`progress_probe.py` 自检保持可运行。
`tests/test_resumable.py` 补充回归：非法预算（含终态任务、布尔与浮点）不改库、
不进入节点；业务失败提交与提交前回滚；非业务异常回滚后只重做未提交工作；
嵌套源、初值、正文与未完成操作数内部暂停；随机化对照 sdk1 进入轨迹与首次错误。
