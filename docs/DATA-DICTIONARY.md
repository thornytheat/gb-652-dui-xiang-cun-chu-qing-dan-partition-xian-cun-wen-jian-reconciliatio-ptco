# 数据字典

| 字段 | 类型 | 可空 | 定义 |
|---|---|---:|---|
| `run_id` | string | 否 | 输入摘要派生的稳定运行编号 |
| `source_line` | integer | 否 | 原文件物理行号 |
| `reason_code` | string | 是 | 隔离或冲突的稳定原因码 |
| `raw_value` | string | 是 | 未归一的原始证据 |
| `normalized_value` | string | 是 | 按项目规则得到的值 |
| `rule_version` | string | 否 | 计算规则版本 |
| `merge_id` | string | 否 | 由源分区、目标分区与基线版本派生的稳定合并单编号 |
| `expected_version` | integer | 否 | 乐观锁版本；与账本当前版本不一致时拒绝写入（STALE_VERSION） |
| `partition` | string | 否 | 对象有效归属分区，小时级 `dt=.../hour=..` 或日级 `dt=...` |
| `sealed` | integer | 否 | 封账标记：1 表示责任数量固定在原小时分区，合并不得搬迁 |
| `transfer_id` | string | 否 | 逐条转账幂等键，由 merge_id 与 key 派生；撤销流水加 `rev-` 前缀 |
| `kept_sealed` | integer | 否 | 转账流水标记：1 表示封账项保留原归属，仅登记证据不改分区 |
| `merge_status` | string | 否 | 合并单状态：RUNNING / COMMITTED / REVERTED / FAILED |

字符串编号逐字保留。金额与计量值用 `Decimal`。时间值带偏移并以 UTC 参与比较。区间统一采用左闭右开。
