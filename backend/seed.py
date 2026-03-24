"""初始化种子数据：演示用采集任务、参数模板、监控事件、知识文章"""
import datetime as _dt
from backend.database import SessionLocal
from backend.models import (
    CollectTask, FlowStep, ParamTemplate,
    MonitorEvent, KnowledgeArticle, SystemMetric,
)
from backend.services.ai_engine import STEP_TEMPLATES
from backend.routers.manual import OPERATIONS_MANUAL

_now = _dt.datetime.utcnow


def seed():
    db = SessionLocal()
    if db.query(CollectTask).count() > 0:
        db.close()
        return

    # --- 采集任务 ---
    tasks_data = [
        ("tf_oh_special_blacklist 全量+增量采集", "tf_oh_special_blacklist", "completed", 100, 6),
        ("tf_f_guarant_contract 增量采集", "tf_f_guarant_contract", "completed", 100, 6),
        ("tf_b_trade_sub 全量初始化", "tf_b_trade_sub", "running", 55, 3),
        ("tf_b_trade_acct 增量还原", "tf_b_trade_acct", "running", 30, 2),
        ("td_s_commpara 全量+增量采集", "td_s_commpara", "pending", 0, 0),
        ("tf_b_paylog 增量采集", "tf_b_paylog", "failed", 40, 3),
    ]
    status_map = {"completed": 6, "running": None, "pending": 0, "failed": None}
    for name, tbl, status, prog, cur in tasks_data:
        t = CollectTask(
            name=name, table_name=tbl, status=status,
            progress=prog, current_step=cur, total_steps=6,
            cluster="hh-fed-sub18",
            namespace="ctg363566671677_hh_fed_sub19_cjzh_cbss_hbase_lb19",
            created_at=_now() - _dt.timedelta(hours=len(tasks_data)),
            started_at=_now() - _dt.timedelta(hours=3) if status != "pending" else None,
            finished_at=_now() if status == "completed" else None,
            error_message="BulkLoad OOM: Region 数过多导致内存不足" if status == "failed" else "",
        )
        db.add(t)
        db.flush()
        for tpl in STEP_TEMPLATES:
            s_status = "completed"
            if status == "pending":
                s_status = "pending"
            elif tpl["step_order"] > cur:
                s_status = "pending"
            elif tpl["step_order"] == cur and status == "running":
                s_status = "running"
            elif status == "failed" and tpl["step_order"] == cur:
                s_status = "failed"
            db.add(FlowStep(
                task_id=t.id, step_order=tpl["step_order"],
                name=tpl["name"], description=tpl["description"],
                sub_steps=tpl["sub_steps"], automation=tpl["automation"],
                status=s_status,
            ))

    # --- 参数模板 ---
    templates = [
        {
            "name": "Flink 标准配置 (tf_oh 系列)",
            "category": "flink",
            "table_pattern": "tf_oh_*",
            "description": "适用于 tf_oh 前缀表的 Flink 增量还原标准配置",
            "params": {
                "sourceBroker": "10.177.64.59:32001,10.177.64.58:32001",
                "resetState": "earliest",
                "setParalizem": "100",
                "hbasezk": "10.177.138.67,10.177.138.68,10.177.138.69",
                "hbaseZookeeperPort": "2181",
                "hbaseParent": "hbasesub19",
                "sourceTopic": "tprds-dc-i-prods-new",
            },
        },
        {
            "name": "Flink 标准配置 (tf_f 系列)",
            "category": "flink",
            "table_pattern": "tf_f_*",
            "description": "适用于 tf_f 前缀表的 Flink 增量还原标准配置",
            "params": {
                "sourceBroker": "10.177.64.59:32001,10.177.64.58:32001",
                "resetState": "earliest",
                "setParalizem": "80",
                "hbasezk": "10.177.138.67,10.177.138.68,10.177.138.69",
                "hbaseZookeeperPort": "2181",
                "hbaseParent": "hbasesub19",
                "sourceTopic": "tprds-dc-i-prods-new",
            },
        },
        {
            "name": "BulkLoad 初始化配置",
            "category": "bulkload",
            "table_pattern": "*",
            "description": "BulkLoad 全量初始化通用脚本参数",
            "params": {
                "script_0x01": "prepare_complete_bulkload_pb.sh",
                "script_0x0A": "prepare_complete_bulkload_pb_0x0A.sh",
                "compression": "gz",
                "default_partition_index": "0",
                "has_partition": "NO",
            },
        },
        {
            "name": "HBase→HDFS 导出配置",
            "category": "export",
            "table_pattern": "*",
            "description": "ETL 导出 HBase 到 HDFS 通用参数",
            "params": {
                "block_size": "4096",
                "buffer_size": "4096",
                "tenant": "ctg363566671677_hh_fed_sub18_cjzh_cbss_lb18",
            },
        },
    ]
    for td in templates:
        db.add(ParamTemplate(**td))

    # --- 监控事件 ---
    events = [
        ("task_completed", "info", "tf_oh_special_blacklist 采集完成", "全流程耗时 1.5h，数据量 2.3M 行", "Platform"),
        ("flink_lag", "warning", "Flink 消费延迟超过 5 分钟", "tf_b_trade_sub Consumer Lag: 15000", "Flink"),
        ("hbase_region_hot", "warning", "HBase Region 热点告警", "Region server 10.177.138.67 负载 > 80%", "HBase"),
        ("bulkload_oom", "critical", "BulkLoad OOM 异常", "tf_b_paylog BulkLoad 因内存不足失败，建议减少预分区数或增加内存", "HBase"),
        ("hdfs_space", "warning", "HDFS 空间使用率 > 85%", "/user/tenants/ctg363566671677 使用 8.5TB / 10TB", "HDFS"),
        ("task_started", "info", "tf_b_trade_acct 增量还原已启动", "Flink Job 已提交至 YARN", "BDI"),
        ("mc_export_ok", "info", "tf_f_guarant_contract MC 导出完成", "数据量校验通过: 1,245,678 行", "MC"),
        ("param_warning", "warning", "Flink 并行度配置偏高", "tf_b_trade_sub setParalizem=200 建议 ≤ 100", "Platform"),
    ]
    for etype, sev, title, detail, src in events:
        db.add(MonitorEvent(
            event_type=etype, severity=sev, title=title,
            detail=detail, source_system=src,
            resolved=1 if sev == "info" else 0,
        ))

    # --- 知识文章 ---
    manual_lines = ["## 订单采集场景（第二个Sheet）全量步骤明细", ""]
    for step in OPERATIONS_MANUAL:
        manual_lines.append(f"### Step {step['step']}：{step['name']}")
        manual_lines.append(f"- 分类：{step.get('category', '—')}")
        manual_lines.append(f"- 系统：{', '.join(step.get('system', []))}")
        manual_lines.append(f"- 自动化：{step.get('automation', '—')}")
        manual_lines.append(f"- 说明：{step.get('description', '')}")
        manual_lines.append("")
        for op in step.get("operations", []):
            manual_lines.append(f"- `{op.get('id', '')}` {op.get('name', '')}（{op.get('type', '—')}）")
            if op.get("command"):
                manual_lines.append(f"  - 命令：`{op.get('command')}`")
            tips = op.get("tips") or []
            if tips:
                manual_lines.append(f"  - 注意：{'；'.join(tips)}")
        manual_lines.append("")
    sheet2_full_content = "\n".join(manual_lines)

    articles = [
        {
            "title": "HBase 预分区数计算规则",
            "category": "best-practice",
            "tags": "HBase,预分区,BulkLoad,初始化",
            "content": (
                "## 计算公式\n\n"
                "```\n预分区数 = ceil(文件大小(GB) × 1024 / Region大小 / 副本系数)\n```\n\n"
                "## 示例\n\n"
                "- 初始化文件 5TB，压缩方式 gz，Region 大小 3GB，副本系数 4\n"
                "- 预分区数 = ceil(5 × 1024 / 3 / 4) = 427 ≈ **430**\n\n"
                "## 注意事项\n\n"
                "- 非压缩文件的 Region 大小取 4GB\n"
                "- 预分区数过多会导致 BulkLoad OOM\n"
                "- 建议先在测试环境验证"
            ),
        },
        {
            "title": "BulkLoad 分隔符选择指南",
            "category": "operation",
            "tags": "BulkLoad,分隔符,0x01,0x0A,初始化",
            "content": (
                "## 两种初始化脚本\n\n"
                "| 脚本 | 字段分隔符 | 行分隔符 | 适用场景 |\n"
                "|------|----------|---------|--------|\n"
                "| `prepare_complete_bulkload_pb.sh` | 0x01 | 0x02 | 标准场景 |\n"
                "| `prepare_complete_bulkload_pb_0x0A.sh` | 0x01 | \\n (0x0A) | 换行符场景 |\n\n"
                "## 判断方法\n\n"
                "使用 `xxd` 命令查看文件前几行的分隔符：\n"
                "```bash\nhead -c 500 init_file.txt | xxd | grep -E '01|02|0a'\n```"
            ),
        },
        {
            "title": "Flink 配置参数详解",
            "category": "reference",
            "tags": "Flink,配置,Kafka,HBase,参数",
            "content": (
                "## 核心参数说明\n\n"
                "| 参数 | 说明 | 示例 |\n"
                "|------|------|------|\n"
                "| `hbaseInfo` | `表名=主键下标\\|字段个数` | `TF_OH=0,4\\|24` |\n"
                "| `tableIndexName` | `表名\\|采集字段列表` | `TF_OH\\|ID,NAME,...` |\n"
                "| `setParalizem` | Flink 并行度 | `100` |\n"
                "| `groupId` | Kafka 消费者组 | `cb2i_r_cjzh_new_xxx` |\n"
                "| `timesKafka` | 消费起始时间 | `202412241120` |\n\n"
                "## hbaseInfo 详解\n\n"
                "格式：`源端Kafka表名=主键下标|采集字段个数`\n"
                "- 主键下标对应 `tableIndexName` 中的字段顺序\n"
                "- 程序根据主键下标拼接 RowKey"
            ),
        },
        {
            "title": "HBase→HDFS 导出 Groovy 脚本编写",
            "category": "operation",
            "tags": "Groovy,导出,HBase,HDFS,日期转换",
            "content": (
                "## 基础步骤\n\n"
                "1. 在配置文件中添加接口：`接口名 块大小 缓冲区 租户ID`\n"
                "2. 新建 Groovy 字段映射脚本\n"
                "3. 含日期字段的需要修改日期转换下标\n\n"
                "## 注意事项\n\n"
                "- **先检查**配置文件中是否已存在该接口，已有则不能重复添加\n"
                "- 多字段日期转换用逗号分隔下标\n"
                "- 启动命令：`sh etl-export-submit.sh 接口名 账期`"
            ),
        },
        {
            "title": "常见故障排查手册",
            "category": "troubleshooting",
            "tags": "故障,OOM,超时,热点,排查",
            "content": (
                "## BulkLoad OOM\n\n"
                "**根因**: 预分区数过多导致 MapReduce 内存不足\n"
                "**解决**: 减少预分区数，或增加 `mapreduce.map.memory.mb`\n\n"
                "## Flink 消费延迟\n\n"
                "**根因**: 并行度不足或数据倾斜\n"
                "**解决**: 增加 `setParalizem` 或检查 Key 分布\n\n"
                "## HBase Region 热点\n\n"
                "**根因**: RowKey 设计不合理导致写入集中\n"
                "**解决**: 手动 Split 热点 Region，优化 RowKey 前缀"
            ),
        },
        {
            "title": "HDFS→MC 导出全流程",
            "category": "operation",
            "tags": "MC,HDFS,外表,内表,导出",
            "content": (
                "## 步骤\n\n"
                "1. **建立外表**: 关联 HDFS 路径的外部表\n"
                "2. **建立内表**: MC 目标存储表\n"
                "3. **执行导出**: `sh hdfscp.sh 接口名 账期 HDFS路径 月 日 表名`\n"
                "4. **验证**: 对比 HDFS 文件行数与 MC 表记录数\n\n"
                "## 参数说明\n\n"
                "```bash\nsh hdfscp.sh D07058 20260121 /user/tenants/.../cbssdata 202601 21 ext_src_d_bcd07058_bak\n```"
            ),
        },
        {
            "title": "订单采集场景第二个Sheet全量内容",
            "category": "reference",
            "tags": "订单采集,Sheet2,全流程,16项操作,自动化",
            "content": sheet2_full_content,
        },
    ]
    for ad in articles:
        db.add(KnowledgeArticle(**ad))

    # --- 系统指标 ---
    import random
    systems = ["Flink", "HBase", "HDFS", "MC"]
    metric_defs = [
        ("throughput", "records/s"), ("latency_p99", "ms"),
        ("cpu_usage", "%"), ("memory_usage", "%"),
    ]
    for sys in systems:
        for mname, unit in metric_defs:
            base = random.uniform(30, 80)
            for i in range(24):
                db.add(SystemMetric(
                    metric_name=mname,
                    metric_value=round(base + random.uniform(-10, 10), 1),
                    unit=unit, source_system=sys,
                    recorded_at=_now() - _dt.timedelta(hours=23 - i),
                ))

    db.commit()
    db.close()
