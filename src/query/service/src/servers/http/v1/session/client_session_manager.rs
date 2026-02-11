// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_cache::Cache;
use databend_common_cache::LruCache;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::user_token::QueryTokenInfo;
use databend_common_meta_app::principal::user_token::TokenType;
use databend_common_meta_app::tenant::Tenant;
use databend_common_users::UserApiProvider;
use databend_storages_common_session::TempTblMgrRef;
use databend_storages_common_session::drop_all_temp_tables;
use log::error;
use log::info;
use log::warn;
use parking_lot::Mutex;
use parking_lot::RwLock;
use sha2::Digest;
use sha2::Sha256;
use tokio::time::Instant;

use crate::servers::http::v1::SessionClaim;
use crate::servers::http::v1::session::consts::TOMBSTONE_TTL;
use crate::servers::http::v1::session::consts::TTL_GRACE_PERIOD_META;
use crate::servers::http::v1::session::consts::TTL_GRACE_PERIOD_QUERY;
use crate::sessions::Session;
use crate::sessions::SessionPrivilegeManager;

pub struct TokenPair {
    pub refresh: String,
    pub session: String,
}

fn hash_token(token: &[u8]) -> String {
    hex::encode_upper(Sha256::digest(token))
}

struct SessionState {
    pub last_refresh_time: Instant,
    pub temp_tbl_mgr: TempTblMgrRef,
}

pub struct ClientSessionManager {
    /// cache of tokens to avoid request for MetaServer on each auth.
    ///
    /// store hash only for hit ratio with limited memory, feasible because:
    /// - token contain all info in itself.
    /// - for eviction, LRU itself is enough, no need to check expired tokens specifically.
    session_tokens: RwLock<LruCache<String, Option<String>>>,
    refresh_tokens: RwLock<LruCache<String, Option<String>>>,

    /// state that
    /// 1. lives across query
    /// 2. only in memory
    /// 3. too large to store in client
    ///
    /// # Ops
    /// add:
    /// - create temp table
    ///
    /// rm:
    ///  - all temp table deleted
    ///  - session closed
    ///  - timeout
    ///
    /// refresh:
    ///  - auth (with min interval)
    session_state: Mutex<BTreeMap<String, SessionState>>,
    pub max_idle_time: Duration,
    pub min_refresh_interval: Duration,
}

impl ClientSessionManager {
    pub fn instance() -> Arc<ClientSessionManager> {
        GlobalInstance::get()
    }

    pub fn state_key(client_session_id: &str, user_name: &str) -> String {
        format!("{user_name}/{client_session_id}")
    }

    fn refresh_token_ttl(&self) -> Duration {
        self.max_idle_time
    }

    fn session_token_ttl(&self) -> Duration {
        self.max_idle_time / 4
    }

    #[async_backtrace::framed]
    pub async fn init(cfg: &InnerConfig) -> Result<()> {
        let mgr = Arc::new(Self {
            session_tokens: RwLock::new(LruCache::with_items_capacity(1024)),
            refresh_tokens: RwLock::new(LruCache::with_items_capacity(1024)),
            max_idle_time: Duration::from_secs(cfg.query.common.http_session_timeout_secs),
            min_refresh_interval: Duration::from_secs(
                (cfg.query.common.http_session_timeout_secs / 10).min(300),
            ),
            session_state: Default::default(),
        });
        GlobalInstance::set(mgr.clone());

        GlobalIORuntime::instance().spawn(async move { Self::check_timeout(mgr).await });
        Ok(())
    }

    async fn check_timeout(self: Arc<Self>) {
        loop {
            let now = Instant::now();
            let mut remained = vec![];
            let mut expired = vec![];
            {
                let guard = self.session_state.lock();
                for (key, session_state) in &*guard {
                    if (now - session_state.last_refresh_time)
                        > self.max_idle_time + self.min_refresh_interval
                    {
                        expired.push((key.clone(), session_state.temp_tbl_mgr.clone()));
                    } else {
                        remained.push(key.clone());
                    }
                }
            };
            {
                let mut guard = self.session_state.lock();
                for (id, _) in expired.iter() {
                    guard.remove(id);
                }
            }
            let num_expired = expired.len();
            for (key, mgr) in expired {
                drop_all_temp_tables_with_logging(&key, mgr, "idle").await;
            }
            let elapsed = now.duration_since(Instant::now());

            if !(remained.is_empty() && num_expired == 0) {
                info!(
                    "[TEMP TABLE] cleanup {num_expired} sessions in {} millisecond, {} remained: {:?}",
                    elapsed.as_millis(),
                    remained.len(),
                    remained
                );
            }
            if elapsed < self.max_idle_time / 4 {
                tokio::time::sleep(self.max_idle_time / 4 - elapsed).await;
            }
        }
    }
    pub async fn try_refresh_state(
        &self,
        tenant: Tenant,
        sid: &str,
        user_name: &str,
    ) -> Result<()> {
        if self.refresh_in_memory_states(sid, user_name) {
            self.refresh_session_handle(tenant, user_name.to_string(), sid)
                .await?;
            info!("refreshing session {}", sid);
        }
        Ok(())
    }

    pub async fn refresh_session_handle(
        &self,
        tenant: Tenant,
        user_name: String,
        client_session_id: &str,
    ) -> Result<()> {
        let client_session_api = UserApiProvider::instance().client_session_api(&tenant);
        client_session_api
            .upsert_client_session_id(
                client_session_id,
                &user_name,
                self.max_idle_time + self.min_refresh_interval + TTL_GRACE_PERIOD_META,
            )
            .await?;
        Ok(())
    }

    /// used for both issue token for new session and renew token for existing session.
    /// currently, when renewing, always return a new refresh token instead of update the TTL,
    /// since now we include expire time in token, which can not be updated.
    pub async fn new_token_pair(
        &self,
        session: &Arc<Session>,
        client_session_id: String,
        old_refresh_token: Option<String>,
        old_session_token: Option<String>,
    ) -> Result<(String, TokenPair)> {
        // those infos are set when the request is authed
        let tenant = session.get_current_tenant();
        let tenant_name = tenant.tenant_name().to_string();
        let user = session.get_current_user()?.name;
        let auth_role = session.privilege_mgr().get_auth_role();
        let client_session_api = UserApiProvider::instance().client_session_api(&tenant);

        // new refresh token
        let now = SystemTime::now();
        let mut claim = SessionClaim::new(
            client_session_id.clone(),
            &tenant_name,
            &user,
            &auth_role,
            self.refresh_token_ttl() + TTL_GRACE_PERIOD_META,
        );
        let refresh_token = claim.encode(TokenType::Refresh);
        let refresh_token_hash = hash_token(refresh_token.as_bytes());
        let token_info = QueryTokenInfo {
            token_type: TokenType::Refresh,
            parent: None,
        };
        client_session_api
            .upsert_token(
                &refresh_token_hash,
                token_info.clone(),
                self.refresh_token_ttl() + TTL_GRACE_PERIOD_META,
                false,
            )
            .await?;
        if let Some(old) = old_refresh_token {
            client_session_api
                .upsert_token(&old, token_info, TOMBSTONE_TTL, true)
                .await?;
        };
        self.refresh_tokens
            .write()
            .insert(refresh_token_hash.clone(), None);

        // session token
        claim.expire_at_in_secs = (now + self.session_token_ttl())
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        claim.nonce = uuid::Uuid::new_v4().to_string();

        let session_token = claim.encode(TokenType::Session);
        let session_token_hash = hash_token(session_token.as_bytes());
        let token_info = QueryTokenInfo {
            token_type: TokenType::Session,
            parent: Some(refresh_token_hash.clone()),
        };
        client_session_api
            .upsert_token(
                &session_token_hash,
                token_info.clone(),
                self.session_token_ttl() + TTL_GRACE_PERIOD_META,
                false,
            )
            .await?;
        self.session_tokens
            .write()
            .insert(session_token_hash, Some(refresh_token_hash));
        if let Some(old) = old_session_token {
            client_session_api
                .upsert_token(&old, token_info, TOMBSTONE_TTL, true)
                .await?;
        };

        // client session id
        client_session_api
            .upsert_client_session_id(
                &client_session_id,
                &claim.user,
                self.max_idle_time + self.min_refresh_interval + TTL_GRACE_PERIOD_META,
            )
            .await?;

        Ok((client_session_id, TokenPair {
            refresh: refresh_token,
            session: session_token,
        }))
    }

    pub(crate) async fn verify_token(self: &Arc<Self>, token: &str) -> Result<SessionClaim> {
        let (claim, token_type) = SessionClaim::decode(token)?;
        if SystemTime::now() > claim.expire_at() + TTL_GRACE_PERIOD_QUERY {
            return match token_type {
                TokenType::Refresh => Err(ErrorCode::RefreshTokenExpired(
                    "Authentication failed: refresh token has expired",
                )),
                TokenType::Session => Err(ErrorCode::SessionTokenExpired(
                    "Authentication failed: session token has expired",
                )),
            };
        }

        let hash = hash_token(token.as_bytes());
        let cache = match token_type {
            TokenType::Refresh => &self.refresh_tokens,
            TokenType::Session => &self.session_tokens,
        };

        if !cache.read().contains(&hash) {
            let tenant = Tenant::new_literal(&claim.tenant);
            match UserApiProvider::instance()
                .client_session_api(&tenant)
                .get_token(&hash)
                .await?
            {
                Some(info) if info.token_type == token_type => {
                    cache.write().insert(hash, info.parent.clone())
                }
                _ => {
                    return match token_type {
                        TokenType::Refresh => Err(ErrorCode::RefreshTokenNotFound(
                            "Authentication failed: refresh token not found in database",
                        )),
                        TokenType::Session => Err(ErrorCode::SessionTokenNotFound(
                            "Authentication failed: session token not found in database",
                        )),
                    };
                }
            };
        };
        Ok(claim)
    }

    pub async fn drop_client_session_state(
        self: &Arc<Self>,
        tenant: &Tenant,
        user_name: &str,
        session_id: &str,
    ) -> Result<()> {
        let client_session_api = UserApiProvider::instance().client_session_api(tenant);
        client_session_api
            .drop_client_session_id(session_id, user_name)
            .await
            .ok();
        let state_key = Self::state_key(session_id, user_name);
        let state = self.session_state.lock().remove(&state_key);
        if let Some(state) = state {
            drop_all_temp_tables_with_logging(&state_key, state.temp_tbl_mgr, "closed").await;
        }
        Ok(())
    }

    pub async fn drop_client_session_token(self: &Arc<Self>, session_token: &str) -> Result<()> {
        let (claim, _) = SessionClaim::decode(session_token)?;
        let client_session_api =
            UserApiProvider::instance().client_session_api(&Tenant::new_literal(&claim.tenant));

        if SystemTime::now() < claim.expire_at() {
            let session_token_hash = hash_token(session_token.as_bytes());
            // should exist after `verify_token`
            let refresh_token_hash =
                if let Some(Some(hash)) = self.session_tokens.write().pop(&session_token_hash) {
                    self.refresh_tokens.write().pop(&hash);
                    Some(hash)
                } else {
                    None
                };
            if let Some(refresh_token_hash) = refresh_token_hash {
                client_session_api
                    .drop_token(&refresh_token_hash)
                    .await
                    .ok();
            }
            client_session_api
                .drop_token(&session_token_hash)
                .await
                .ok();
        };
        Ok(())
    }

    pub fn refresh_in_memory_states(&self, client_session_id: &str, user_name: &str) -> bool {
        let key = Self::state_key(client_session_id, user_name);
        let mut guard = self.session_state.lock();
        if let Entry::Occupied(mut entry) = guard.entry(key) {
            let now = Instant::now();
            if now.duration_since(entry.get().last_refresh_time)
                > ClientSessionManager::instance().min_refresh_interval
            {
                entry.get_mut().last_refresh_time = Instant::now();
                return true;
            }
        }
        false
    }

    pub fn on_query_start(&self, client_session_id: &str, user_name: &str, session: &Arc<Session>) {
        let key = Self::state_key(client_session_id, user_name);
        let mut guard = self.session_state.lock();
        guard.entry(key).and_modify(|e| {
            if e.temp_tbl_mgr.lock().is_empty() {
                warn!("[TEMP TABLE] session={client_session_id} empty temp_tbl_mgr in ClientSessionManager.");
            } else {
                info!("[TEMP TABLE] session={client_session_id} restore temp_tbl_mgr");
                session.set_temp_tbl_mgr(e.temp_tbl_mgr.clone());
            }
        });
    }

    pub fn add_temp_tbl_mgr(&self, prefix: String, temp_tbl_mgr: TempTblMgrRef) {
        let mut guard = self.session_state.lock();
        let state = SessionState {
            last_refresh_time: Instant::now(),
            temp_tbl_mgr,
        };
        if guard.insert(prefix.clone(), state).is_none() {
            info!("[TEMP TABLE] session={prefix} added to ClientSessionManager");
        }
    }

    pub fn remove_temp_tbl_mgr(&self, prefix: String, temp_tbl_mgr: &TempTblMgrRef) {
        let mut guard = self.session_state.lock();
        let is_empty = { temp_tbl_mgr.lock().is_empty() };
        if is_empty {
            let removed_session_state = guard.remove(&prefix);
            // Defensive check that drop session state contains no temp tables
            if let Some(removed_session_state) = removed_session_state {
                assert!({ removed_session_state.temp_tbl_mgr.lock().is_empty() });
            }
            // all temp table dropped by user, data should have been removed when executing drop.
            info!("[TEMP TABLE] session={prefix} removed from ClientSessionManager");
        }
    }

    /// Get all temporary tables from all sessions
    pub fn get_all_temp_tables(
        &self,
    ) -> Result<Vec<(String, databend_common_meta_app::schema::TableInfo)>> {
        let mut all_temp_tables = Vec::new();
        let session_states = self.session_state.lock();
        for (session_key, session_state) in session_states.iter() {
            let temp_tables = session_state.temp_tbl_mgr.lock().list_tables()?;
            for table in temp_tables {
                all_temp_tables.push((session_key.clone(), table));
            }
        }
        Ok(all_temp_tables)
    }
}

pub async fn drop_all_temp_tables_with_logging(
    user_name_session_id: &str,
    mgr: TempTblMgrRef,
    reason: &str,
) {
    let start = std::time::Instant::now();

    match drop_all_temp_tables(user_name_session_id, mgr, reason).await {
        Ok(_) => {
            let duration = start.elapsed().as_millis();
            info!(
                "[TEMP TABLE] session={} clean up completed in {}ms",
                user_name_session_id, duration
            );
        }
        Err(e) => {
            let duration = start.elapsed().as_millis();
            error!(
                "[TEMP TABLE] session={} clean up failed after {}ms: error={:?}",
                user_name_session_id, duration, e
            );
        }
    }
}
