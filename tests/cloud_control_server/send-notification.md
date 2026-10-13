# 主动通知 SQL 入口

`send_notification` table function 通过 Cloud Control 的 `EnqueueNotification` RPC 受理通知。Query 不访问 webhook、不渲染模板，也不等待 webhook 投递。成功仅表示 Cloud Control 已持久化受理；网络超时或响应丢失时可能已受理，重新执行可能重复发送。

## 接口

```sql
CALL system$send_notification('oncall', 'Query errors detected');
SELECT * FROM send_notification('oncall', 'Query errors detected');
```

要求全局 `SUPER` 权限，与 notification integration DDL 一致。两个位置参数必须为字符串；integration 名称非空，message 为 1–262,144 UTF-8 字节。integration 是否存在、是否 enabled 由 Cloud Control 受理时验证。

`CALL` 沿用已有 table function 改写机制，但清空对外输出列并丢弃输出，因此成功时没有结果集、不返回 delivery ID。直接 `SELECT` 也支持，具有内部 `result String` 占位列，但无结果行。不要把它作为提供数据的关系使用，也不要依赖 `LIMIT 0`、过滤、JOIN 等关系改写来控制副作用；需要条件发送时使用脚本控制流。

现有 CALL parser 参数为字符串，不能假设支持动态表达式。脚本编译器目前拒绝所有 CALL；脚本中使用 table function SELECT 及现有变量绑定语法：

```sql
EXECUTE IMMEDIATE $$
BEGIN
    LET message := 'Query errors detected';
    IF TRUE THEN
        SELECT * FROM send_notification('oncall', :message);
    END IF;
END;
$$;
```

## 执行和错误语义

- 使用 Local 分布、单 source 的 `TableFunctionTemplate`，首次执行 source 时发出一次 RPC；创建 table、绑定、读取 partitions 和普通 EXPLAIN 不调用 RPC。EXPLAIN ANALYZE 会实际执行查询，不属于无副作用的 EXPLAIN。
- binder 禁用本次查询的结果缓存，避免重复 SQL 被缓存吞掉通知。
- tenant 和 query ID 来自可信 Query context；query ID 作为 `query_id`，不是幂等键。RPC metadata 同时携带 tenant、user、query ID 和 notification client version。proto 中 `query_id` 是 optional，Cloud Control 允许省略；Query 入口始终携带当前 query ID。
- 沿用 Cloud Control 配置地址和 timeout，通过 `make_request` 设置 RPC deadline。不自动重试 RPC，不降级为同步 webhook。
- gRPC 失败或 response error 均转为 SQL 错误。旧 Cloud Control 的 `Unimplemented` 明确报告不支持主动通知。
- 后台投递失败不会回溯修改 SQL 或 Task 结果。Query 的 `system.notification_history` 暂未暴露 `query_id`；可以直接查询 `system.notification_history` 的现有结果；按 Query ID 精确定位使用 Cloud Control 日志/history RPC/数据库，SQL history 的 Query ID 字段展示属于后续工作。

## 对象与状态流

```text
Query(query_id)
  -> EnqueueNotification
  -> notification_delivery(id=delivery_id, integration_id, query_id, pending)
  -> Worker claim(processing, attempt_count, lease)
  -> HTTP attempts(http_attempt, same delivery_id)
  -> Atomic terminal delivery + USER notification_history(query_id)
```

这是持久化队列加后台 worker 的异步链路，不是流处理引擎。入队响应的 `delivery_id` 为本次投递任务 ID，不返回 SQL 结果；`integration_id` 绑定既有配置对象，值等于既有 `notification.notification_id`，旧管理 RPC 不改名。一个 Query/integration 可以对应多条 delivery；HTTP 重试沿用同一 delivery，history 有独立主键，不能按两个自增 ID 相等关联。`query_id` 是关联键，不是幂等键。

`unfinished` 仅是 pending + processing 的指标口径，不是新的队列状态。

## 验证

```bash
cargo check -p databend-query --lib
cargo test -p databend-common-cloud-control enqueue_acceptance_errors_and_no_retry
cargo test -p databend-query --lib validates_arguments_without_sending
cargo test -p databend-query --test it test_send_notification_planning_is_result_free_and_uncacheable
cargo build -p databend-binaries --bin databend-query
```

配好 Query 的 Cloud Control 地址并启动 `tests/cloud_control_server/simple_server.py` 后运行定向 SQL 回归：

```bash
./target/debug/databend-sqllogictests --handlers http --run_file task_send_notification.test
```

mock 的 integration 和动态 history 按 `tenant_id` 隔离；两条既有 JSON history fixture 是共享的只读测试数据，不会因 DROP 被删除。删除 integration 仅清理当前 tenant 的对应动态 history。

除了单文件回归，还应启用 Query 的 `--internal-enable-sandbox-tenant`，按 CI 的并发方式验证完整 `task` suite，避免只跑单文件漏掉跨 tenant 污染：

```bash
./target/debug/databend-sqllogictests --run_suite task --handlers http,hybrid --enable_sandbox --parallel 8
```

mock 自身的 ID 分配、tenant/删除隔离、fixture 保留、optional query ID 及拒绝不写 history 可单独验证（需要匹配现有生成代码的 Python gRPC/protobuf 依赖）：

```bash
cd tests/cloud_control_server
python3 -m unittest -v test_send_notification
```

SQL 回归使用 mock history 统计受理次数；不做真实 HTTP 投递。覆盖 CALL、重复 SELECT/结果缓存、条件脚本变量、EXPLAIN 无副作用、参数错误、disabled/missing integration、受理错误和旧服务不支持。权限回归位于 `tests/suites/0_stateless/20+_others/20_0029_send_notification_privilege_check.sh`，在未启用 Cloud Control 的普通环境也应先报权限错误。
