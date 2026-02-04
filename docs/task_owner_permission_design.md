# Task Owner Permission Design

## 背景

当前 Databend Task 的权限模型需要与 Snowflake 保持一致：
- Task 内部的 SQL 应该以 **owner role** 的权限执行
- 如果 owner 没有执行 SQL 的权限，task 执行应该失败
- 如果 owner role 被删除，task 执行应该失败
- 如果 ownership 被转移，task 应该以新 owner 的权限执行

## 当前实现分析

### databend-query 端

1. **创建 Task 时**：当前 role 被设置为 task owner
   - `interpreter_task_create.rs`: 调用 `grant_ownership` 设置 ownership
   - Owner 信息存储在 meta 中

2. **权限检查**：
   - `privilege_access.rs`: 检查用户是否有 CreateTask/AlterTask/DropTask 等权限
   - 检查用户是否是 task 的 owner

### databend-cloud 端

1. **存储 Task**：
   - `task.Owner` 字段存储在数据库中
   - 创建时从 `request.Owner` 获取

2. **执行 Task**：
   - `task_executor.go`: 调用 `queryRunner.Run(ctx, tenant, warehouse, task.Owner, ...)`
   - `task_query_runner.go`: 设置 `c.Role = task.Owner`

3. **认证方式**：
   - K3d 测试环境：JWT token 中 `Role = "account_admin"`（固定）
   - 生产环境：使用 `AccessTokenFile`（internal token）

### 问题

**`task.Owner` 没有被真正用于权限检查**：
- 虽然 `c.Role = task.Owner` 被设置
- 但 JWT token 中的 Role 或 internal token 的权限覆盖了它
- 导致 task 总是以高权限用户执行，绕过了 owner 的权限检查

## 方案对比

### 方案 A：修改 Cloud 端 JWT Token 生成

**实现**：
- 修改 `signK3dTestAuthToken` 和生产环境的 token 生成逻辑
- 使用 `task.Owner` 作为 JWT token 中的 `Role`

**优点**：
- 利用现有的权限检查机制
- 不需要修改 databend-query 端
- Revoke 权限后立即生效

**缺点**：
- 需要修改 Cloud 端认证逻辑
- 可能影响其他使用 internal token 的功能

**代码修改**：
```go
// task_query_runner.go
func (r *taskQueryRunnerImpl) Run(...) *taskQueryRunError {
    // 生成以 task.Owner 为 role 的 JWT token
    token := signTaskOwnerToken(tenantID, role)  // role = task.Owner

    queryConf := query.NewQueryConfigs(tenantID, warehouseName, role, sessionSettings)
    clientCfg := query.BuildDatabendConfigWithToken(r.conf, queryConf, token)
    // ...
}
```

### 方案 B：Cloud 端执行前检查权限

**实现**：
- 在执行 task SQL 前，以 owner 身份执行 `EXPLAIN <sql>`
- 如果失败，说明 owner 没有权限

**优点**：
- 不需要修改认证逻辑
- 可以精确检查每个 SQL 的权限

**缺点**：
- 额外的 API 调用开销
- 需要维护两套连接（owner 连接 + internal 连接）

**代码修改**：
```go
// task_query_runner.go
func (r *taskQueryRunnerImpl) checkOwnerPermission(tenantID, owner string, sqls []string) error {
    // 以 owner 身份连接
    ownerToken := signTaskOwnerToken(tenantID, owner)
    ownerClient := godatabend.NewAPIClientFromConfig(ownerCfg)

    // 检查每个 SQL 的权限
    for _, sql := range sqls {
        _, err := ownerClient.StartQuery(ctx, "EXPLAIN " + sql, nil)
        if err != nil {
            return fmt.Errorf("owner %s has no permission: %w", owner, err)
        }
    }
    return nil
}
```

### 方案 C：databend-query 端添加权限检查 API

**实现**：
- 添加新的 gRPC API：`CheckTaskPermission(tenant, task_name)`
- Cloud 端执行前调用此 API

**优点**：
- 权限检查逻辑集中在 databend-query 端
- 可以利用 meta 中的 ownership 信息

**缺点**：
- 需要修改 proto 定义
- 需要修改 databend-query 和 cloud 两端

**Proto 定义**：
```protobuf
message CheckTaskPermissionRequest {
    string tenant_id = 1;
    string task_name = 2;
}

message CheckTaskPermissionResponse {
    bool has_permission = 1;
    string error_message = 2;
}
```

## 推荐方案

**推荐方案 A**：修改 Cloud 端 JWT Token 生成

理由：
1. **最简单**：利用现有的权限检查机制
2. **最可靠**：权限检查由 databend-query 自动完成
3. **最一致**：与 Snowflake 的行为一致

### 实现步骤

1. **修改 Cloud 端 token 生成**：
   - 创建新函数 `signTaskOwnerToken(tenantID, ownerRole string)`
   - 在 JWT token 中设置 `Role = ownerRole`

2. **修改 task_query_runner.go**：
   - 使用 `signTaskOwnerToken` 生成 token
   - 不再使用 internal token

3. **处理历史 task**：
   - 对于没有 owner 的历史 task，使用 `account_admin` 作为默认 role

4. **同步 ownership 变更**：
   - 当 `GRANT OWNERSHIP ON TASK` 执行时，需要同步更新 cloud 中的 `task.Owner`
   - 或者每次执行前从 meta 获取最新 owner

## 测试计划

1. **单元测试**：
   - 测试 token 生成逻辑
   - 测试权限检查逻辑

2. **集成测试**：
   - 用户有 CREATE TASK 权限，无 SELECT 权限 → task 执行失败
   - Owner 权限被 revoke → task 执行失败
   - Owner role 被删除 → task 执行失败
   - Ownership 转移 → task 以新 owner 权限执行

## 相关文件

### databend-query
- `src/query/service/src/interpreters/interpreter_task_create.rs`
- `src/query/service/src/interpreters/interpreter_privilege_grant.rs`
- `src/query/service/src/interpreters/access/privilege_access.rs`

### databend-cloud
- `cloudcontrol/pkg/task/scheduler/service/task_executor.go`
- `cloudcontrol/pkg/task/scheduler/service/task_query_runner.go`
- `cloudcontrol/pkg/managers/query_manager/query_manager.go`
