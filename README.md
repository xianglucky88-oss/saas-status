# SaaS Status

SaaS 产品状态页、维护公告和版本变更监测的离线实现，回答值班问题：**某个服务在某个观察时刻，被哪些组件、哪几起事故以什么级别影响，证据是什么。**

## 模块职责

### 既有基线（保持可用）

- `sources.py`：读取状态页 JSON/RSS 统一输入。
- `normalize.py`：统一组件、时间和事件状态。
- `merge.py`：按服务、组件和事件键合并多来源更新。
- `storage.py` / `pipeline.py`：合并后事件快照与内容哈希，调度入口。

### 服务影响计算（本次新增）

- `clock.py`：ISO-8601 时间归一化与生效区间判断（半开区间 `[from, until)`）。
- `topology.py`：版本化拓扑模型与图引擎（反向依赖传播、SCC 循环检测、跨代名称解析、悬挂边识别）。
- `incidents.py`：从不可变观察记录重构事故时间窗（含恢复后复发、迟到消息修正）。
- `engine.py`：纯函数计算引擎，输出状态、影响级别、贡献事故、组件路径与证据。
- `impact_storage.py`：拓扑代、观察证据、计算 run/task/result 的 SQLite 存储，全部按自然键幂等。
- `impact.py`：查询门面（即时查询、可续跑批量计算）。

## 数据模型与时间语义

### 版本化拓扑（本地 JSON 输入）

```json
{
  "generation_id": "topo-2026w46",
  "effective_from": "2026-11-11T00:00:00Z",
  "nodes": [
    {"id": "svc-checkout", "kind": "service", "name": "Checkout"},
    {"id": "comp-billing", "kind": "component", "name": "Settlement"}
  ],
  "edges": [
    {"kind": "owns", "source": "svc-checkout", "target": "comp-billing"},
    {"kind": "depends_on", "source": "svc-checkout", "target": "comp-billing"}
  ]
}
```

- 节点用稳定 `id` 标识，`name` 只在某一代内有效；边支持 `depends_on` 与 `owns`。
- 新代入库时，旧代所有开放行在新代 `effective_from` 处闭合，**历史行原样保留**。
- 改名（如 Billing → Settlement）只写入新代；历史时刻查询仍解析出旧名称，当前时刻查询用新名称，二者通过稳定 id 关联，并在结果中同时给出 `component_name`（当前代名称）与 `component_name_at_event`（事件当时名称）。
- 同一组件被多个服务依赖时，反向 `depends_on` 传播会让所有可达服务受影响；`owns` 提供归属解释与兜底路径。

### 事件时间与拓扑生效时间如何共同决定传播范围

对观察时刻 `T` 的一次查询：

1. **拓扑快照**：取所有满足 `effective_from <= T < effective_until` 的节点和边，构成 T 时刻图。名称按该代解析；事件中的历史名称通过跨代别名表（同一稳定 id 的历代名称）关联，关联不上才判未判定。
2. **事故时间窗**：每条原始观察不可变地保存在 `event_observations`；同一 `event_id` 的观察按 `updated_at` 排序重构区间——非 resolved 观察开窗，resolved 观察在其 `updated_at` 闭窗，之后再出现非 resolved 则新开一窗；无 resolved 观察则窗口开放。
3. **联合判定**：事故窗口覆盖 T 时，在 T 时刻图上从组件反向遍历依赖边，所有可达服务即为受影响服务。一个组件恢复只闭它自己的窗口，其他开放窗口的事故继续让服务显示受影响。
4. **迟到消息**：晚到的观察按自然键插入而不是覆盖，只改变该事故的重构窗口与标签；每条贡献证据带 `late_evidence`（该观察 `updated_at > T`），整起贡献带 `label_uses_late_evidence`。其他事故的窗口不受影响。
5. **影响级别**：服务级别取 T 时刻所有活动事故标签的最高级（none < minor < major < critical），状态映射为 `operational / degraded / critical`；无法判定时为 `undetermined`。

### 可识别的未判定原因码

| code | 含义 |
| --- | --- |
| `no_topology` | 查询时刻没有任何生效的拓扑代 |
| `unresolved_name` | 服务/组件名称在当前代及历代别名中都无法定位，或一对多产生歧义 |
| `dangling_reference` | 拓扑边引用了快照中不存在的节点 |
| `cycle_dependency` | 依赖图中存在可达的强连通环（Tarjan SCC 检测，附环节点） |
| `unknown_state` | 观察携带规范化表之外的状态 |

未判定结果仍返回已能确定的贡献事故与路径，原因列表按被查询服务关联，不混入无关事件。

### 中断恢复

- 拓扑与观察写入按 `(generation_id, node_id)`、`(event_key, updated_at, source_ref)` 等自然键幂等，重复入库只报告重复数。
- 批量计算以 `compute_runs / compute_tasks / service_status_results` 记录每个 `(run, service, as_of)` 任务，单任务独立事务提交；进程被杀留下的 `running` 与 `failed`、`pending` 任务在 resume 时重新认领，已完成任务跳过。

## 用法

旧命令与旧事件查询保持不变：

```bash
PYTHONPATH=src python3 -m saas_status.cli --fixture fixtures/status.json --database /tmp/demo.sqlite3
PYTHONPATH=src python3 -m saas_status.cli events --database /tmp/demo.sqlite3
```

新增拓扑、观察、查询与批量计算：

```bash
PYTHONPATH=src python3 -m saas_status.cli ingest-topology --input fixtures/demo/topology_v1.json --database /tmp/demo.sqlite3
PYTHONPATH=src python3 -m saas_status.cli ingest-events   --input fixtures/demo/events_initial.json --database /tmp/demo.sqlite3
PYTHONPATH=src python3 -m saas_status.cli query  --service "Checkout" --as-of "2026-11-10T06:00:00Z" --database /tmp/demo.sqlite3
PYTHONPATH=src python3 -m saas_status.cli query-all --as-of "2026-11-10T06:00:00Z" --database /tmp/demo.sqlite3
PYTHONPATH=src python3 -m saas_status.cli run --services "Web Shop,Checkout,Customer Portal" --as-of "2026-11-10T06:00:00Z" --run-id run-1 --database /tmp/demo.sqlite3
PYTHONPATH=src python3 -m saas_status.cli resume --run-id run-1 --database /tmp/demo.sqlite3
PYTHONPATH=src python3 -m saas_status.cli run-status --run-id run-1 --database /tmp/demo.sqlite3
```

端到端离线演示（共享组件、两起重叠事故、局部恢复、拓扑换代、迟到消息、中断恢复、未判定原因）：

```bash
python3 -I scripts/demo.py
```

## 测试

无外部依赖，标准库 `unittest` 即可（也兼容 pytest）：

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```
