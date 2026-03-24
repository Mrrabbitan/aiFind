"""AI 引擎：流程编排 / 参数推荐 / 异常检测 / 知识检索核心能力"""
from __future__ import annotations
import math, re, hashlib, datetime as _dt
from typing import Dict, List, Any


STEP_TEMPLATES: List[Dict[str, Any]] = [
    {
        "step_order": 1,
        "name": "确定源端表结构及 MC 表结构",
        "description": "AI 自动解析源端 Schema，推断字段映射关系",
        "automation": "auto",
        "sub_steps": ["拉取源端元数据", "生成字段映射", "生成 MC 目标表 DDL"],
    },
    {
        "step_order": 2,
        "name": "新建 HBase 表",
        "description": "基于数据量智能计算预分区数，自动生成建表命令",
        "automation": "auto",
        "sub_steps": ["计算预分区数", "生成建表命令", "执行建表"],
    },
    {
        "step_order": 3,
        "name": "全量初始化（历史全量文件）",
        "description": "自动编排 HDFS 操作链：创建目录→权限→上传→BulkLoad",
        "automation": "semi-auto",
        "sub_steps": [
            "创建 HDFS 初始化目录",
            "调整目录权限",
            "上传初始化文件到 HDFS",
            "执行 BulkLoad 初始化入库",
        ],
    },
    {
        "step_order": 4,
        "name": "Flink 增量还原入库",
        "description": "模板化生成 Flink 配置（20+ 参数自动填充），API 创建 BDI 流程",
        "automation": "auto",
        "sub_steps": ["生成 Flink 配置文件", "创建 BDI 采集还原流程"],
    },
    {
        "step_order": 5,
        "name": "HBase 数据导出到 HDFS",
        "description": "自动生成 Groovy 映射脚本，启动导出并智能校验",
        "automation": "semi-auto",
        "sub_steps": [
            "修改提交参数配置文件",
            "生成 Groovy 字段映射脚本",
            "启动导出脚本",
            "自动校验导出结果",
        ],
    },
    {
        "step_order": 6,
        "name": "HDFS 导出到 MC",
        "description": "AI 生成外表/内表 DDL，执行导出并断言校验数据量",
        "automation": "semi-auto",
        "sub_steps": [
            "生成并执行外表 DDL",
            "生成并执行内表 DDL",
            "执行 HDFS→MC 导出脚本",
            "数据量断言校验",
        ],
    },
]


def calculate_pre_regions(file_size_bytes: int, compression: str = "gz") -> int:
    size_gb = file_size_bytes / (1024 ** 3)
    region_size = 3 if compression == "gz" else 4
    replicas = 4
    return max(10, math.ceil(size_gb * 1024 / region_size / replicas))


def generate_hbase_create_cmd(table_name: str, zk_hosts: str, zk_parent: str,
                               pre_regions: int, compression: str, namespace: str,
                               zk_port: int = 2181) -> str:
    full_table = f"{namespace}:{table_name}"
    return (
        f"java -cp /data/disk01/shangyunOrder/lib/QueryHbaseTable.jar "
        f"cn.com.bonc.CreateTable {zk_hosts} {zk_parent} {pre_regions} "
        f"{compression} {full_table} {zk_port}"
    )


def generate_flink_config(table_name: str, field_list: list[str],
                           pk_indexes: str, namespace: str,
                           cluster: str = "hh-fed-sub18") -> Dict[str, str]:
    safe_name = table_name.lower()
    return {
        "checkPointPath": f"hdfs://{cluster}/user/tenants/.../shangyunCheckpoint/checkPoint0_{safe_name}",
        "sourceBroker": "10.177.64.59:32001,10.177.64.58:32001",
        "resetState": "earliest",
        "groupId": f"cb2i_r_cjzh_new_{safe_name}",
        "userName": "cbss_2i_k",
        "password": "******",
        "jobName": safe_name,
        "sourceTopic": "tprds-dc-i-prods-new",
        "timesKafka": _dt.datetime.now().strftime("%Y%m%d%H%M"),
        "setParalizem": "100",
        "hbaseInfo": f"{table_name.upper()}={pk_indexes}|{len(field_list)}",
        "tableIndexName": f"{table_name.upper()}|{','.join(field_list)}",
        "hbasezk": "10.177.138.67,10.177.138.68,10.177.138.69",
        "hbaseZookeeperPort": "2181",
        "hbaseParent": "hbasesub19",
        "namespace": namespace,
        "tableEnd": "",
        "defaultFS": cluster,
    }


def generate_bulkload_cmd(table_name: str, init_path: str, hfile_path: str,
                           namespace: str, field_count: int,
                           pk_index: str = "0", partition_index: int = 0,
                           field_indexes: str = "") -> str:
    if not field_indexes:
        field_indexes = ",".join(str(i) for i in range(field_count))
    return (
        f'nohup sh prepare_complete_bulkload_pb.sh '
        f'{init_path}/ '
        f'{hfile_path}/{table_name}/ '
        f'*{table_name}* '
        f'{field_count} '
        f'"{pk_index}" '
        f'{partition_index} NO '
        f'{namespace}:{table_name} '
        f'"{field_indexes}" '
        f'> log/{table_name}.log 2>&1 &'
    )


def validate_params(config: Dict[str, str]) -> List[Dict[str, str]]:
    issues: List[Dict[str, str]] = []
    zk = config.get("hbasezk", "")
    if zk and not re.match(r'^(\d{1,3}\.){3}\d{1,3}(,(\d{1,3}\.){3}\d{1,3})*$', zk):
        issues.append({"field": "hbasezk", "level": "error", "message": "ZK 地址格式不合法"})

    par = config.get("setParalizem", "")
    if par and (not par.isdigit() or int(par) > 500):
        issues.append({"field": "setParalizem", "level": "warning",
                       "message": "并行度建议 ≤ Kafka 分区数×2 且不超过 500"})

    topic = config.get("sourceTopic", "")
    if topic and not re.match(r'^[a-zA-Z0-9\-_]+$', topic):
        issues.append({"field": "sourceTopic", "level": "error", "message": "Topic 名称含非法字符"})

    if config.get("password") and config["password"] != "******":
        issues.append({"field": "password", "level": "warning", "message": "密码不应明文存储"})

    if not issues:
        issues.append({"field": "", "level": "success", "message": "全部参数校验通过"})
    return issues


def recommend_params(table_name: str) -> Dict[str, Any]:
    lower = table_name.lower()
    if "tf_oh" in lower:
        parallelism = 100
        topic = "tprds-dc-i-prods-new"
    elif "tf_f" in lower:
        parallelism = 80
        topic = "tprds-dc-i-prods-new"
    else:
        parallelism = 50
        topic = "tprds-dc-i-prods-new"

    return {
        "recommended_parallelism": parallelism,
        "recommended_topic": topic,
        "recommended_group_id": f"cb2i_r_cjzh_new_{lower}",
        "recommended_compression": "gz",
        "recommended_pre_regions": 10,
        "confidence": 0.92,
        "basis": "基于表名模式匹配历史最优配置",
    }


def analyze_anomaly(metrics: List[float]) -> Dict[str, Any]:
    if not metrics:
        return {"status": "no_data"}
    import statistics
    mean = statistics.mean(metrics)
    stdev = statistics.stdev(metrics) if len(metrics) > 1 else 0
    anomalies = [i for i, v in enumerate(metrics) if abs(v - mean) > 3 * stdev] if stdev else []
    return {
        "mean": round(mean, 2),
        "stdev": round(stdev, 2),
        "anomaly_indexes": anomalies,
        "status": "anomaly_detected" if anomalies else "normal",
    }


def search_knowledge(query: str, articles: list) -> list:
    query_lower = query.lower()
    scored = []
    for a in articles:
        score = 0
        text = f"{a.get('title','')} {a.get('tags','')} {a.get('content','')}".lower()
        for word in query_lower.split():
            if word in text:
                score += text.count(word)
        if score > 0:
            scored.append({**a, "_score": score})
    scored.sort(key=lambda x: x["_score"], reverse=True)
    return scored[:10]
