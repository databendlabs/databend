# Task Permission and Internal Token Analysis (Databend + Cloud Platform)

## Scope
This note documents how internal tokens are used across Databend Cloud Platform and how Databend resolves
roles during task operations. It summarizes the evidence from code and provides feasible options with
pros/cons and risks.

## Confirmed Code Facts (With Evidence)

### CloudControl uses a token file by default
File: `cloudcontrol/pkg/conf/config.go:10`
```go
const defaultDatabendTokenFile = "/var/run/secrets/kubernetes.io/serviceaccount/token"
```

### CloudControl builds Databend API client with AccessTokenFile
File: `cloudcontrol/pkg/managers/query_manager/query_manager.go:102`
```go
if appConf != nil {
	if strings.ToLower(appConf.ProviderName()) == "k3d" {
		c.AccessToken = signK3dTestAuthToken(req.TenantID)
	} else {
		c.AccessTokenFile = appConf.DatabendTokenFilePath()
	}
}
```

### WebAPI JWT claims include ensure_user roles
File: `webapi/clients/jwt_signer/claims.go:10`
```go
type DatabendClaims struct {
	jwt.RegisteredClaims
	TenantID   string `json:"tenant_id"`
	EnsureUser struct {
		DefaultRole string   `json:"default_role"`
		Roles       []string `json:"roles"`
	} `json:"ensure_user"`
}
```

### Dataintegration cron signs account_admin JWT
File: `webapi/worker/dataintegration_cron/task.go:409`
```go
jwtClaims := jwt_signer.NewDatabendClaims(accountInfo.Email, tenantID, "account_admin")
jwtSigner := jwt_signer.NewSigner(&jwtConfig)
jwtToken, err := jwtSigner.Sign(jwtClaims)
```

### Manage-service trusts WebAPI JWKS and configures cloudcontrol serviceaccount user
File: `cloud/charts/manage-service/values.yaml:32`
```yaml
jwtKeyFiles:
  - http://webapi-service.cloud-platform.svc:8080/.well-known/jwks.json
users:
  - name: system:serviceaccount:databend-cloudcontrol:databend-cloudcontrol
    authType: jwt
```

### Operator also injects JWKS and cloudcontrol user
File: `operator/pkg/configs/config.go:424`
```go
queryConfig.Users = append(queryConfig.Users, BuiltInUser{
	Name:     CloudControlUserName,
	AuthType: "jwt",
})
```

### Databend role semantics include account_admin owning all roles
File: `src/query/service/src/sessions/session_privilege_mgr.rs:38`
```rust
/// - There're two special roles in the role role hierarchy: PUBLIC and ACCOUNT_ADMIN. PUBLIC is by default
///   granted to every role, and ACCOUNT_ADMIN is by default have all the roles granted.
```

### Databend available roles are auth_role or user grants
File: `src/query/service/src/sessions/session_privilege_mgr.rs:270`
```rust
let roles = match self.session_ctx.get_auth_role() {
    Some(auth_role) => vec![auth_role],
    None => {
        let current_user = self.get_current_user()?;
        let mut roles = current_user.grants.roles();
        if let Some(current_role) = self.get_current_role() {
            roles.push(current_role.name);
        }
        roles
    }
};
```

### validate_available_role only checks membership
File: `src/query/service/src/sessions/session_privilege_mgr.rs:360`
```rust
let available_roles = self.get_all_available_roles().await?;
let role = available_roles.iter().find(|r| r.name == role_name);
```

### JWT role becomes auth_role
File: `src/query/service/src/auth.rs:275`
```rust
session.set_authed_user(user, jwt.custom.role).await?;
```

### Task owner is current role at creation time
File: `src/query/service/src/interpreters/task/cloud.rs:74`
```rust
let owner = ctx
    .get_current_role()
    .unwrap_or_default()
    .identity()
    .to_string();
```

## Direct Findings
1. CloudControl uses `DATABEND_TOKEN_FILE` by default; it does not authenticate as `task.Owner`.
   (Evidence: `cloudcontrol/pkg/conf/config.go:10`, `cloudcontrol/pkg/managers/query_manager/query_manager.go:102`)
2. WebAPI can issue Databend JWTs with `ensure_user.roles`, and dataintegration cron uses `account_admin`.
   (Evidence: `webapi/clients/jwt_signer/claims.go:10`, `webapi/worker/dataintegration_cron/task.go:409`)
3. Databend treats `account_admin` as having all roles, and `validate_available_role` only checks
   membership in the available role list. If the internal token maps to account_admin, the check always
   succeeds.
   (Evidence: `src/query/service/src/sessions/session_privilege_mgr.rs:38`,
   `src/query/service/src/sessions/session_privilege_mgr.rs:270`,
   `src/query/service/src/sessions/session_privilege_mgr.rs:360`)
4. The repo does not show a definitive override of `DATABEND_TOKEN_FILE` for cloudcontrol; therefore the
   runtime token content cannot be confirmed from code alone.

## Feasible Options (With Pros/Cons)

### Option A: Keep internal token; enforce only task metadata permissions
**Idea**
- Maintain current CloudControl token flow and enforce task permission checks on Databend side
  (CREATE/ALTER/EXECUTE/SHOW).

**Pros**
- Minimal changes to CloudControl and WebAPI.
- Fastest to ship.

**Cons**
- SQL execution still runs with internal token privileges.
- If internal token corresponds to account_admin, `validate_available_role` becomes ineffective.

**Risks**
- Data access may exceed task.Owner intent.

**Code basis**
- CloudControl uses `AccessTokenFile` (`cloudcontrol/pkg/managers/query_manager/query_manager.go:102`).
- account_admin has all roles (`src/query/service/src/sessions/session_privilege_mgr.rs:38`).

### Option B: Issue per-task JWT with auth_role = task.Owner (recommended)
**Idea**
- CloudControl requests a short-lived JWT where `custom.role = task.Owner`.
- Databend will treat `auth_role` as the only available role.

**Pros**
- `validate_available_role` is strictly scoped to task.Owner.
- Clear and enforceable execution semantics.

**Cons**
- WebAPI JWT signer does not currently include `role` in claims.

**Risks**
- Requires new signer capability and secure distribution.

**Code basis**
- auth_role overrides available roles (`src/query/service/src/sessions/session_privilege_mgr.rs:270`).
- JWT role is set as auth_role (`src/query/service/src/auth.rs:275`).

### Option C: Issue per-task JWT with ensure_user.roles = [task.Owner]
**Idea**
- Keep existing WebAPI signer; mint short-lived JWT with ensure_user roles only.

**Pros**
- Uses existing JWT signer schema (`webapi/clients/jwt_signer/claims.go:10`).

**Cons**
- If the underlying user already has other roles, available roles may still include them.

**Risks**
- Role leakage unless a dedicated single-role user is used.

**Code basis**
- available roles are derived from user grants (`src/query/service/src/sessions/session_privilege_mgr.rs:270`).

### Option D: Execute with user-level token (caller identity)
**Idea**
- Store creator identity on task and use that user JWT to execute.

**Pros**
- Most accurate permission semantics.

**Cons**
- Requires significant data/model changes and token lifecycle handling.

**Risks**
- User disabled/role changes require re-evaluation of stored tasks.

## Open Items
- Confirm what `DATABEND_TOKEN_FILE` is in the cloudcontrol runtime (serviceaccount token or WebAPI JWT).
- If a JWT is used, confirm whether it includes account_admin or a constrained role set.
