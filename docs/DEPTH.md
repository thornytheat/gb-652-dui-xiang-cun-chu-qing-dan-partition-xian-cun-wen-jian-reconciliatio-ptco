# 深度登记 对象存储清单与分区现存文件对账

        - 规模：中型
        - 核心规则：路径规范化后按 key、size 与摘要逐项比对，目录标记、空分区和缺失对象不得混为一类。
        - 使用者与工作流：导入随仓样例，逐模块复核，最后导出单一证据清单。

        ## 功能模块

        | 模块 | 使用者动作 | 入口 | 核心文件 | 测试 |
        |---|---|---|---|---|
        | M1 清单采信 | 运行样例并复核证据 | `/api/manifests/import` / `/manifest` | `app/modules/m1_manifest/core.py` `app/modules/m1_manifest/service.py` | `tests/test_m1_manifest.py` |
| M2 路径契约 | 运行样例并复核证据 | `/api/paths/normalize` / `/paths` | `app/modules/m2_paths/core.py` `app/modules/m2_paths/service.py` | `tests/test_m2_paths.py` |
| M3 对象核验 | 运行样例并复核证据 | `/api/objects/verify` / `/objects` | `app/modules/m3_objects/core.py` `app/modules/m3_objects/service.py` | `tests/test_m3_objects.py` |
| M4 分区判定 | 运行样例并复核证据 | `/api/partitions` / `/partitions` | `app/modules/m4_partitions/core.py` `app/modules/m4_partitions/service.py` | `tests/test_m4_partitions.py` |
| M5 差异对账 | 运行样例并复核证据 | `/api/reconcile` / `/reconcile` | `app/modules/m5_reconcile/core.py` `app/modules/m5_reconcile/service.py` | `tests/test_m5_reconcile.py` |
| M6 批次导出 | 运行样例并复核证据 | `/api/export/differences.csv` / `/runs` | `app/modules/m6_runs/core.py` `app/modules/m6_runs/service.py` | `tests/test_m6_runs.py` |
| M7 分区合并 | 接入现存对象、封账、合并、撤销并回读差异 | `/api/merges` / `/merges` | `app/modules/m7_merges/core.py` `app/modules/m7_merges/service.py` `app/modules/m7_merges/store.py` | `tests/test_m7_core.py` `tests/test_m7_service.py` `tests/test_m7_api.py` |

        ## 陷阱登记

        | TRAP | 现象 | 朴素实现 | 本项目处理 | 处理位置 | 测试 |
        |---|---|---|---|---|---|
        | TRAP-02 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m1_manifest/core.py` | `tests/test_m1_manifest.py::test_trap_1` |
| TRAP-05 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m2_paths/core.py` | `tests/test_m2_paths.py::test_trap_1` |
| TRAP-08 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m3_objects/core.py` | `tests/test_m3_objects.py::test_trap_1` |
| TRAP-09 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m4_partitions/core.py` | `tests/test_m4_partitions.py::test_trap_1` |
| TRAP-14 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m5_reconcile/core.py` | `tests/test_m5_reconcile.py::test_trap_1` |
| TRAP-16 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m6_runs/core.py` | `tests/test_m6_runs.py::test_trap_1` |
| TRAP-18 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m1_manifest/core.py` | `tests/test_m1_manifest.py::test_trap_2` |
| TRAP-21 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m2_paths/core.py` | `tests/test_m2_paths.py::test_trap_2` |
| TRAP-22 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m3_objects/core.py` | `tests/test_m3_objects.py::test_trap_2` |
| TRAP-23 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m4_partitions/core.py` | `tests/test_m4_partitions.py::test_trap_2` |
| TRAP-26 | 合并直接搬账会改写封账历史，两边累计又重复 | 把所有对象迁入日分区，或原小时与日分区各计一次 | 封账归属保留原小时仅留流水，未封账项注销原归属转入目标，总量守恒 | `app/modules/m7_merges/service.py` | `tests/test_m7_service.py::test_merge_moves_unsealed_and_keeps_sealed` |
| TRAP-27 | 合并与封账并发，过期版本仍被接受 | 无条件按计划转账 | 乐观版本号在任何写入前校验，过期返回 STALE_VERSION 且失败保留旧版；转账瞬间再查封账状态 | `app/modules/m7_merges/service.py` `app/modules/m7_merges/store.py` | `tests/test_m7_service.py::test_seal_landing_between_retries_keeps_object_in_hour` `tests/test_m7_api.py::test_stale_version_returns_422_and_keeps_old_version` |
| TRAP-28 | 跨分区写入中断后重试重复转账 | 整批回滚或整批重做 | 逐条独立事务，transfer_id 幂等，同 merge_id 恢复只续做未完成项 | `app/modules/m7_merges/service.py` | `tests/test_m7_service.py::test_merge_resumes_after_batch_interrupt_without_double_transfer` `tests/test_m7_service.py::test_merge_retry_is_idempotent_after_transfer_failure` |
| TRAP-29 | 撤销合并误推后补对象 | 按当前日分区对象整体退回 | 只退回合并流水内且仍在目标分区的对象，后补对象沿自身有效归属保留 | `app/modules/m7_merges/service.py` | `tests/test_m7_service.py::test_revert_keeps_sealed_and_backfilled_objects` |

        ## 对账规则

        - 输入记录数 = 有效记录数 + 隔离记录数 + 空记录数。
        - 核心数值或事件数在转换、判定和导出之间保持守恒。
        - 同一输入摘要重跑复用原运行编号，不增加结果行。
        - 分区合并只迁移归属，不改 key、size 与摘要；当前差异归新日分区，封账责任数量留在原小时。
        - 合并与撤销前后有效归属总量守恒；逐条转账幂等，中断恢复与重试不产生重复流水。
        - 合并与封账携带 expected_version，过期版本拒绝写入并保留旧版；不做补数领取。

        ## 已知不覆盖

        - 生产系统连接、身份认证和外部密钥不在演示范围内。
