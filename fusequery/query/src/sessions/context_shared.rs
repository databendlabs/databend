use crate::configs::Config;
use std::sync::Arc;
use common_infallible::RwLock;
use crate::sessions::{Settings, Session};
use crate::clusters::ClusterRef;
use crate::datasources::DataSource;
use common_progress::Progress;
use common_runtime::Runtime;
use uuid::Uuid;
use common_exception::Result;

/// Data that needs to be shared in a query context.
/// This is very useful, for example, for queries:
///     USE database_1;
///     SELECT
///         (SELECT scalar FROM table_name_1) AS scalar_1,
///         (SELECT scalar FROM table_name_2) AS scalar_2,
///         (SELECT scalar FROM table_name_3) AS scalar_3
///     FROM table_name_4;
/// For each subquery, they will share a runtime, session, progress, init_query_id
pub(in sessions) struct FuseQueryContextShared {
    pub(in sessions) conf: Config,
    pub(in sessions) progress: Arc<Progress>,
    pub(in sessions) session: Arc<Session>,
    pub(in sessions) runtime: Arc<RwLock<Runtime>>,
    pub(in sessions) init_query_id: Arc<RwLock<String>>,
}

impl FuseQueryContextShared {
    pub fn try_create(conf: Config, session: Arc<Session>) -> Result<Arc<FuseQueryContextShared>> {
        Ok(Arc::new(FuseQueryContextShared {
            conf,
            init_query_id: Arc::new(RwLock::new(Uuid::new_v4().to_string())),
            progress: Arc::new(Progress::create()),
            session: session,
            runtime: Arc::new(RwLock::new(Runtime::with_worker_threads(
                settings.get_max_threads()? as usize,
            )?)),
        }))
    }
}
