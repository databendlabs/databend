use std::collections::HashMap;

use common_planners::CreateDatabasePlan;
use common_planners::DatabaseEngineType;

use crate::engine::mem_engine::MemEngine;
use crate::protobuf::Db;

#[test]
fn test_mem_engine_create_database() -> anyhow::Result<()> {
    let eng = MemEngine::create();

    let mut eng = eng.lock().unwrap();

    {
        // create db foo
        let p = CreateDatabasePlan {
            if_not_exists: false,
            db: "foo".into(),
            engine: DatabaseEngineType::Local,
            options: HashMap::new(),
        };

        let rst = eng.create_database(p);
        assert_eq!(0, rst.unwrap());
        assert_eq!(
            Db {
                db_id: 0,
                table_name_to_id: HashMap::new(),
                tables: HashMap::new(),
            },
            eng.get_database("foo".into()).unwrap()
        );
    }

    {
        // create db bar
        let p = CreateDatabasePlan {
            if_not_exists: false,
            db: "bar".into(),
            engine: DatabaseEngineType::Local,
            options: HashMap::new(),
        };
        let rst = eng.create_database(p);
        assert_eq!(1, rst.unwrap());
        assert_eq!(
            Db {
                db_id: 1,
                table_name_to_id: HashMap::new(),
                tables: HashMap::new(),
            },
            eng.get_database("bar".into()).unwrap()
        );
    }

    {
        // create db bar failure
        let p = CreateDatabasePlan {
            if_not_exists: true,
            db: "bar".into(),
            engine: DatabaseEngineType::Local,
            options: HashMap::new(),
        };
        let rst = eng.create_database(p);
        assert_eq!("database exists", format!("{}", rst.err().unwrap()));
        assert_eq!(
            Db {
                db_id: 1,
                table_name_to_id: HashMap::new(),
                tables: HashMap::new(),
            },
            eng.get_database("bar".into()).unwrap(),
            "got the previous bar"
        );
    }
    Ok(())
}
