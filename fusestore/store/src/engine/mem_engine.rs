use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use common_planners::CreateDatabasePlan;

use crate::protobuf::Db;
//use crate::protobuf::Table;

// MemEngine is a prototype storage that is primarily used for testing purposes.
// TODO add table APIs
pub struct MemEngine {
    dbs: HashMap<String, Db>,
    next_id: i64,
}

impl MemEngine {
    #[allow(dead_code)]
    pub fn create() -> Arc<Mutex<MemEngine>> {
        let e = MemEngine {
            dbs: HashMap::new(),
            next_id: 0,
        };
        Arc::new(Mutex::new(e))
    }

    #[allow(dead_code)]
    pub fn create_database(&mut self, plan: CreateDatabasePlan) -> anyhow::Result<i64> {
        // TODO: support plan.engine plan.options
        let curr = self.dbs.get(&plan.db);
        if curr.is_some() && plan.if_not_exists {
            return Err(anyhow::anyhow!("database exists"));
        }
        let db_id = self.next_id;
        self.next_id += 1;
        self.dbs.insert(
            plan.db,
            Db {
                db_id,
                table_name_to_id: HashMap::new(),
                tables: HashMap::new(),
            },
        );

        Ok(db_id)
    }

    #[allow(dead_code)]
    pub fn get_database(&self, db: String) -> anyhow::Result<Db> {
        let x = self
            .dbs
            .get(&db)
            .ok_or_else(|| anyhow::anyhow!("database not found"))?;
        Ok(x.clone())
    }
}
