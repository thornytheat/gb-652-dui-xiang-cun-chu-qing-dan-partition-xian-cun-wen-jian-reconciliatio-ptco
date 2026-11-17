REASONS = {
    "BAD_COLUMNS": "字段数量不符", "EMPTY_VALUE": "必要值为空",
    "DUPLICATE_CONFLICT": "同一业务键取值冲突", "TRUNCATED": "尾记录不完整",
    "INVALID_VALUE": "值不符合定义", "STALE_VERSION": "提交版本已过期",
    "PARTITION_SEALED": "分区已封账", "PARTITION_EMPTY": "分区无现存对象不能封账",
    "MERGE_NOT_FOUND": "合并单不存在", "MERGE_NOT_COMMITTED": "合并单未提交不能撤销",
    "MERGE_REVERTED": "合并单已撤销", "TRANSFER_FAILED": "跨分区转账中断",
    "TOTAL_MISMATCH": "总量守恒校验失败",
}

def reason(code: str) -> str:
    return REASONS.get(code, "未登记原因")
