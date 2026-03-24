"""操作手册 API：订单采集流程分步、对标矩阵与 hbaseTest 样例资产。"""

import hashlib
from pathlib import Path
from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/api/manual", tags=["manual"])
PROJECT_ROOT = Path(__file__).resolve().parents[2]
HBASE_TEST_DIR = PROJECT_ROOT / "hbaseTest"

OPERATIONS_MANUAL = [
    {
        "step": 1,
        "name": "确定源端表结构及 MC 表结构",
        "category": "规划",
        "system": ["MC"],
        "automation": "auto",
        "description": "比对源端与目标端字段映射关系，确认表结构和字段类型",
        "operations": [
            {
                "id": "1.1",
                "name": "拉取源端元数据",
                "type": "auto",
                "description": "AI 自动连接源端数据库，获取表的 Schema 信息（字段名、类型、注释）",
                "command": None,
                "params": [],
                "tips": ["确认源端数据库连接信息", "注意分区字段和主键的识别"],
                "risk": "low",
            },
            {
                "id": "1.2",
                "name": "生成字段映射",
                "type": "auto",
                "description": "AI 根据源端 Schema 自动推断目标端字段映射关系",
                "command": None,
                "params": [],
                "tips": ["检查日期类型字段是否需要格式转换", "确认主键字段的组合方式"],
                "risk": "low",
            },
            {
                "id": "1.3",
                "name": "生成 MC 目标表 DDL",
                "type": "auto",
                "description": "基于映射关系自动生成 MC 建表语句",
                "command": None,
                "params": [],
                "tips": ["确认分区策略", "检查字段长度是否足够"],
                "risk": "low",
            },
        ],
    },
    {
        "step": 2,
        "name": "新建 HBase 表",
        "category": "建表",
        "system": ["HBase"],
        "automation": "auto",
        "description": "基于数据量智能计算预分区数，执行 HBase 建表命令",
        "operations": [
            {
                "id": "2.1",
                "name": "计算预分区数",
                "type": "auto",
                "description": "根据初始化文件大小计算预分区数。公式：预分区数 = ceil(文件大小GB × 1024 / Region大小 / 副本系数)。注意：非压缩文件 Region 大小取 4GB。",
                "command": None,
                "params": [
                    {"name": "file_size", "label": "初始化文件大小", "unit": "GB", "required": True},
                    {"name": "compression", "label": "压缩方式", "options": ["gz", "none"], "default": "gz"},
                    {"name": "region_size", "label": "Region大小", "unit": "GB", "default": 3},
                    {"name": "replicas", "label": "副本系数", "default": 4},
                ],
                "tips": [
                    "初始化文件为非压缩文件时，源端初始化文件存储与HBase预分区数相关",
                    "示例：初始化文件 5TB → 预分区数 = 5×1024/3/4 ≈ 430",
                ],
                "risk": "medium",
            },
            {
                "id": "2.2",
                "name": "执行建表命令",
                "type": "auto",
                "description": "使用 CreateTable jar 创建 HBase 表",
                "command": "java -cp /data/disk01/shangyunOrder/lib/QueryHbaseTable.jar cn.com.bonc.CreateTable {zk_hosts} {zk_parent} {pre_regions} {compression} {namespace}:{table_name} {zk_port}",
                "params": [
                    {"name": "zk_hosts", "label": "ZK地址", "default": "10.177.138.67,10.177.138.68,10.177.138.69"},
                    {"name": "zk_parent", "label": "ZK父节点", "default": "/hbasesub19"},
                    {"name": "pre_regions", "label": "预分区数", "required": True},
                    {"name": "compression", "label": "压缩方式", "default": "gz"},
                    {"name": "namespace", "label": "命名空间", "required": True},
                    {"name": "table_name", "label": "表名", "required": True},
                    {"name": "zk_port", "label": "ZK端口", "default": "2181"},
                ],
                "tips": ["建表前确认命名空间已存在", "确认ZK地址连通性"],
                "risk": "medium",
            },
        ],
    },
    {
        "step": 3,
        "name": "全量初始化（历史全量文件）",
        "category": "初始化",
        "system": ["HDFS", "HBase"],
        "automation": "semi-auto",
        "description": "将历史全量文件上传至 HDFS 并通过 BulkLoad 导入 HBase",
        "operations": [
            {
                "id": "3.1",
                "name": "创建 HDFS 初始化目录",
                "type": "auto",
                "description": "在 HDFS 上创建初始化文件存放目录",
                "command": "hadoop fs -mkdir -p /user/tenants/{tenant}/{workspace}/work/{work_group}/init/data{date}/{table_name}/new",
                "params": [
                    {"name": "tenant", "label": "租户ID", "required": True},
                    {"name": "workspace", "label": "工作空间", "required": True},
                    {"name": "work_group", "label": "工作组", "required": True},
                    {"name": "date", "label": "日期(yyyyMMdd)", "required": True},
                    {"name": "table_name", "label": "表名", "required": True},
                ],
                "tips": [],
                "risk": "low",
            },
            {
                "id": "3.2",
                "name": "调整 HDFS 目录权限",
                "type": "auto",
                "description": "确保目录有正确的读写权限",
                "command": "hadoop fs -chmod -R 755 /user/tenants/{tenant}/{workspace}/work/{work_group}/init/data{date}/{table_name}/new",
                "params": [],
                "tips": ["权限不足会导致后续 BulkLoad 失败"],
                "risk": "low",
            },
            {
                "id": "3.3",
                "name": "上传初始化文件到 HDFS",
                "type": "manual",
                "description": "将本地初始化文件上传至 HDFS 指定路径",
                "command": "hadoop fs -put {local_file} /user/tenants/{tenant}/{workspace}/work/{work_group}/init/data{date}/{table_name}/new",
                "params": [
                    {"name": "local_file", "label": "本地文件路径", "required": True},
                ],
                "tips": ["大文件上传耗时较长，建议后台执行", "确认磁盘空间充足"],
                "risk": "medium",
            },
            {
                "id": "3.4",
                "name": "执行 BulkLoad 初始化入库",
                "type": "semi-auto",
                "description": "通过 BulkLoad 方式将数据批量导入 HBase",
                "command": 'nohup sh {script} {init_path}/ {hfile_path}/{table_name}/ *{table_name}* {field_count} "{pk_index}" {partition_index} {has_partition} {namespace}:{table_name} "{field_indexes}" > log/{table_name}.log 2>&1 &',
                "params": [
                    {
                        "name": "script",
                        "label": "初始化脚本",
                        "options": [
                            "prepare_complete_bulkload_pb.sh (分隔符0x01,行0x02)",
                            "prepare_complete_bulkload_pb_0x0A.sh (分隔符0x01,行\\n)",
                        ],
                        "required": True,
                    },
                    {"name": "init_path", "label": "HDFS初始化文件路径", "required": True},
                    {"name": "hfile_path", "label": "HFile输出路径", "required": True},
                    {"name": "table_name", "label": "表名", "required": True},
                    {"name": "field_count", "label": "字段个数", "required": True},
                    {"name": "pk_index", "label": "主键字段下标", "default": "0"},
                    {"name": "partition_index", "label": "分区字段下标", "default": "0"},
                    {"name": "has_partition", "label": "是否有分区字段", "options": ["NO", "YES"], "default": "NO"},
                    {"name": "namespace", "label": "HBase命名空间:表名", "required": True},
                    {"name": "field_indexes", "label": "采集字段下标", "required": True},
                ],
                "tips": [
                    "字段分隔符0x01、行分隔符0x02 使用 prepare_complete_bulkload_pb.sh",
                    "字段分隔符0x01、行分隔符\\n 使用 prepare_complete_bulkload_pb_0x0A.sh",
                    "预分区数过多会导致 BulkLoad OOM",
                    "可用 xxd 命令检查文件分隔符类型",
                ],
                "risk": "high",
            },
        ],
    },
    {
        "step": 4,
        "name": "Flink 增量还原入库",
        "category": "增量采集",
        "system": ["Flink", "Kafka", "BDI", "HBase"],
        "automation": "auto",
        "description": "配置 Flink 消费 Kafka 增量数据并写入 HBase",
        "operations": [
            {
                "id": "4.1",
                "name": "新建 Flink 配置文件",
                "type": "auto",
                "description": "生成包含 Kafka、HBase、HDFS 等 20+ 参数的 Flink 配置文件",
                "command": None,
                "params": [
                    {"name": "checkPointPath", "label": "Checkpoint路径", "required": True},
                    {"name": "sourceBroker", "label": "Kafka Broker地址", "required": True},
                    {"name": "groupId", "label": "消费者组ID", "required": True},
                    {"name": "sourceTopic", "label": "Kafka Topic", "required": True},
                    {"name": "timesKafka", "label": "消费起始时间(yyyyMMddHHmm)", "required": True},
                    {"name": "setParalizem", "label": "并行度", "default": "100"},
                    {"name": "hbaseInfo", "label": "HBase表信息(表名=主键下标|字段数)", "required": True},
                    {"name": "tableIndexName", "label": "表名|采集字段列表", "required": True},
                    {"name": "hbasezk", "label": "HBase ZK地址", "required": True},
                    {"name": "namespace", "label": "HBase命名空间", "required": True},
                ],
                "tips": [
                    "hbaseInfo格式：表名=主键下标|字段个数，主键下标对应tableIndexName字段顺序",
                    "tableIndexName格式：表名|字段1,字段2,...（Kafka消息中的字段名）",
                    "程序根据主键下标拼接RowKey，按字段顺序以\\u0001拼接值",
                    "tableEnd如果HBase表名有后缀写在这里",
                ],
                "risk": "medium",
            },
            {
                "id": "4.2",
                "name": "在 BDI 上建立采集还原流程",
                "type": "semi-auto",
                "description": "在 BDI 平台上创建 Flink 采集还原流程",
                "command": None,
                "params": [
                    {"name": "task_name", "label": "任务名称", "required": True},
                    {"name": "platform", "label": "运行平台", "default": "采集整合联邦18集群1.11"},
                    {"name": "run_mode", "label": "运行模式", "default": "YARN_PER"},
                    {"name": "main_class", "label": "主类名", "required": True},
                    {"name": "jar_url", "label": "主类jar的HTTP地址", "required": True},
                    {"name": "config_path", "label": "配置文件地址", "required": True},
                ],
                "tips": ["确认集群资源充足", "检查 Kafka Topic 是否存在且有数据"],
                "risk": "medium",
            },
        ],
    },
    {
        "step": 5,
        "name": "HBase 数据导出到 HDFS",
        "category": "数据导出",
        "system": ["HBase", "HDFS"],
        "automation": "semi-auto",
        "description": "将 HBase 数据通过 ETL 导出到 HDFS",
        "operations": [
            {
                "id": "5.1",
                "name": "修改提交参数配置文件",
                "type": "manual",
                "description": "在配置文件中添加导出接口信息",
                "command": None,
                "params": [
                    {"name": "interface_id", "label": "接口名", "required": True},
                    {"name": "block_size", "label": "块大小", "default": "4096"},
                    {"name": "buffer_size", "label": "缓冲区大小", "default": "4096"},
                    {"name": "tenant", "label": "租户ID", "required": True},
                ],
                "tips": [
                    "先查看配置文件中有没有该接口，已有则不能重复添加",
                    "格式：接口名 块大小 缓冲区大小 租户ID",
                ],
                "risk": "medium",
            },
            {
                "id": "5.2",
                "name": "新建 Groovy 字段映射脚本",
                "type": "semi-auto",
                "description": "创建 Groovy 脚本用于 HBase 到 HDFS 的字段映射和转换",
                "command": None,
                "params": [
                    {"name": "date_field_indexes", "label": "日期字段下标(逗号分隔)", "required": False},
                ],
                "tips": [
                    "含日期字段转换的需要修改日期转换字段下标",
                    "多字段用逗号分隔",
                ],
                "risk": "medium",
            },
            {
                "id": "5.3",
                "name": "启动导出脚本",
                "type": "auto",
                "description": "执行 ETL 导出命令将 HBase 数据导出到 HDFS",
                "command": "sh etl-export-submit.sh {interface_id} {date}",
                "params": [
                    {"name": "interface_id", "label": "接口名", "required": True},
                    {"name": "date", "label": "账期(yyyyMMdd)", "required": True},
                ],
                "tips": [],
                "risk": "low",
            },
            {
                "id": "5.4",
                "name": "校验导出结果",
                "type": "manual",
                "description": "检查导出是否成功，验证字段是否正确",
                "command": None,
                "params": [],
                "tips": ["检查HDFS文件是否生成", "抽样验证字段值是否正确", "对比数据条数"],
                "risk": "low",
            },
        ],
    },
    {
        "step": 6,
        "name": "HDFS 导出到 MC",
        "category": "数据推送",
        "system": ["HDFS", "MC"],
        "automation": "semi-auto",
        "description": "将 HDFS 数据推送到 MC（MaxCompute）",
        "operations": [
            {
                "id": "6.1",
                "name": "建立外表",
                "type": "semi-auto",
                "description": "创建关联 HDFS 路径的 MC 外部表",
                "command": None,
                "params": [],
                "tips": ["外表关联的 HDFS 路径必须存在且有数据"],
                "risk": "low",
            },
            {
                "id": "6.2",
                "name": "建立内表",
                "type": "semi-auto",
                "description": "创建 MC 目标存储内表",
                "command": None,
                "params": [],
                "tips": ["确认分区策略与字段类型"],
                "risk": "low",
            },
            {
                "id": "6.3",
                "name": "执行 HDFS→MC 导出脚本",
                "type": "auto",
                "description": "运行导出脚本将 HDFS 数据写入 MC 表",
                "command": "sh hdfscp.sh {interface_id} {date} {hdfs_path} {month} {day} {mc_table}",
                "params": [
                    {"name": "interface_id", "label": "接口名", "required": True},
                    {"name": "date", "label": "账期(yyyyMMdd)", "required": True},
                    {"name": "hdfs_path", "label": "HDFS数据路径", "required": True},
                    {"name": "month", "label": "月份(yyyyMM)", "required": True},
                    {"name": "day", "label": "日(dd)", "required": True},
                    {"name": "mc_table", "label": "MC目标表名", "required": True},
                ],
                "tips": ["示例：sh hdfscp.sh D07058 20260121 /user/tenants/.../cbssdata 202601 21 ext_src_d_bcd07058_bak"],
                "risk": "medium",
            },
            {
                "id": "6.4",
                "name": "验证 MC 数据量",
                "type": "manual",
                "description": "对比 HDFS 文件行数与 MC 表记录数，确认数据一致性",
                "command": None,
                "params": [],
                "tips": ["数据量不一致时排查是否有脏数据被过滤", "检查分区是否正确"],
                "risk": "low",
            },
        ],
    },
]

BENCHMARK_MATRIX = [
    {"dimension": "数据采集集成", "alibaba": "多源批流一体/CDC", "tencent": "多引擎EMR/DLC", "bytedance": "BitSail插件化", "current": "Flink+BulkLoad模板", "gap": "P1", "plan": "Phase 2"},
    {"dimension": "流程编排调度", "alibaba": "DAG可视化/周期调度", "tencent": "WeData工作流", "bytedance": "调度引擎/Gödel", "current": "6步顺序编排", "gap": "P0", "plan": "Phase 1"},
    {"dimension": "参数配置管理", "alibaba": "环境变量/连接管理", "tencent": "参数模板/版本", "bytedance": "配置中心化", "current": "模板+AI推荐", "gap": "P1", "plan": "Phase 2"},
    {"dimension": "质量监控", "alibaba": "DQC规则引擎/阻断", "tencent": "76系统规则/字段级监控", "bytedance": "基线监控/全链路", "current": "基础事件+异常检测", "gap": "P0", "plan": "Phase 1"},
    {"dimension": "智能告警", "alibaba": "智能基线/预测告警", "tencent": "告警阻断下游", "bytedance": "基线监控覆盖80%+", "current": "严重度分级", "gap": "P0", "plan": "Phase 1"},
    {"dimension": "根因分析", "alibaba": "血缘追踪", "tencent": "全链路血缘", "bytedance": "依赖链分析", "current": "无", "gap": "P0", "plan": "Phase 1"},
    {"dimension": "知识管理", "alibaba": "数据地图", "tencent": "数据知识库/资产", "bytedance": "数据发现", "current": "知识库+关键词检索", "gap": "P1", "plan": "Phase 2"},
    {"dimension": "AI Copilot", "alibaba": "AI辅助开发", "tencent": "暂无公开", "bytedance": "暂无公开", "current": "无", "gap": "P2", "plan": "Phase 3"},
    {"dimension": "设计系统", "alibaba": "Ant Design", "tencent": "TDesign", "bytedance": "Arco Design", "current": "Tailwind散写", "gap": "P1", "plan": "Phase 2"},
    {"dimension": "权限审计", "alibaba": "RAM/审计", "tencent": "CAM/操作审计", "bytedance": "内部权限", "current": "无", "gap": "P1", "plan": "Phase 2"},
    {"dimension": "成本分析", "alibaba": "资源优化", "tencent": "冷热分层/成本治理", "bytedance": "资源利用率优化", "current": "无", "gap": "P2", "plan": "Phase 3"},
]

SCRIPT_FILES = [
    "prepare_complete_bulkload_pb.sh",
    "prepare_complete_bulkload_pb_0x0A.sh",
    "新建采集还原配置文件.txt",
    "导出需要修改和新增配置文件以及执行的脚本.txt",
    "hdfs到外表执行脚本.txt",
]

JAR_FILES = [
    "OggKafkaToHbase_PBV3.jar",
    "OggKafkaToHbase_PBV5.jar",
    "QueryHbaseTable.jar",
    "hbasesub19-utils-jar-with-dependencies.jar",
]

JAR_EXAMPLES = {
    "QueryHbaseTable.jar": "java -cp QueryHbaseTable.jar cn.com.bonc.CreateTable <zk_hosts> <zk_parent> <pre_regions> <compression> <namespace:table> <zk_port>",
    "OggKafkaToHbase_PBV3.jar": "在 BDI/Flink 任务中配置主类与参数，作为 Kafka -> HBase 增量还原作业的运行包",
    "OggKafkaToHbase_PBV5.jar": "在 BDI/Flink 任务中配置主类与参数，作为 Kafka -> HBase 增量还原作业（新版协议）运行包",
    "hbasesub19-utils-jar-with-dependencies.jar": "用于 HBase 辅助工具调用，常用于查询/导出/校验链路中的依赖包",
}


def _total_operations() -> int:
    return sum(len(s["operations"]) for s in OPERATIONS_MANUAL)


def _manual_summary() -> dict:
    risk_breakdown: dict[str, int] = {}
    type_breakdown: dict[str, int] = {}
    automation_steps: dict[str, int] = {}
    for step in OPERATIONS_MANUAL:
        automation_steps[step["automation"]] = automation_steps.get(step["automation"], 0) + 1
        for op in step["operations"]:
            r = op["risk"]
            risk_breakdown[r] = risk_breakdown.get(r, 0) + 1
            t = op["type"]
            type_breakdown[t] = type_breakdown.get(t, 0) + 1
    return {
        "step_count": len(OPERATIONS_MANUAL),
        "categories": [s["category"] for s in OPERATIONS_MANUAL],
        "step_automation": automation_steps,
        "operation_type_breakdown": type_breakdown,
        "risk_breakdown": risk_breakdown,
    }


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _load_samples() -> dict:
    scripts = []
    jars = []
    if HBASE_TEST_DIR.exists():
        for name in SCRIPT_FILES:
            p = HBASE_TEST_DIR / name
            if p.exists() and p.is_file():
                scripts.append(
                    {
                        "name": name,
                        "path": str(p),
                        "size_bytes": p.stat().st_size,
                        "content": p.read_text(encoding="utf-8", errors="replace"),
                        "usage": "可直接在操作手册中作为执行模板复制使用",
                    }
                )
        for name in JAR_FILES:
            p = HBASE_TEST_DIR / name
            if p.exists() and p.is_file():
                jars.append(
                    {
                        "name": name,
                        "path": str(p),
                        "size_bytes": p.stat().st_size,
                        "sha256": _sha256(p),
                        "usage_example": JAR_EXAMPLES.get(name, "请参考任务平台中的主类参数配置"),
                    }
                )
    return {"scripts": scripts, "jars": jars, "folder": str(HBASE_TEST_DIR)}


@router.get("")
def get_manual():
    return {
        "steps": OPERATIONS_MANUAL,
        "total_operations": _total_operations(),
        "summary": _manual_summary(),
    }


@router.get("/benchmark")
def get_benchmark():
    return {"matrix": BENCHMARK_MATRIX}


@router.get("/samples")
def get_sample_assets():
    data = _load_samples()
    return {
        "folder": data["folder"],
        "script_count": len(data["scripts"]),
        "jar_count": len(data["jars"]),
        "scripts": data["scripts"],
        "jars": data["jars"],
    }


@router.get("/steps/{step_id}")
def get_manual_step(step_id: int):
    for step in OPERATIONS_MANUAL:
        if step["step"] == step_id:
            return step
    raise HTTPException(status_code=404, detail=f"未找到步骤: {step_id}")
