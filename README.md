# SaaS Status

SaaS 产品状态页、维护公告和版本变更监测的离线实现。

## 单一职责

- `sources.py`：读取状态页 JSON/RSS 统一输入。
- `normalize.py`：统一组件、时间和事件状态。
- `merge.py`：按服务、组件和事件键合并多来源更新。
- `storage.py`：保存事件快照和内容哈希。
- `pipeline.py`：负责调度上述模块。

```bash
PYTHONPATH=src python3 -m saas_status.cli --fixture fixtures/status.json --database /tmp/saas-status.sqlite3
```
