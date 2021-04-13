// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
use std::collections::HashMap;

use crate::engine::mem_engine::MemEngine;
use crate::protobuf::CmdCreateDatabase;
use crate::protobuf::Db;

#[test]
fn test_mem_engine_create_database() -> anyhow::Result<()> {
    // TODO check generated ver
    let eng = MemEngine::create();

    let mut eng = eng.lock().unwrap();

    let cmdfoo = CmdCreateDatabase {
        db_name: "foo".into(),
        db: Some(Db {
            db_id: -1,
            ver: -1,
            table_name_to_id: HashMap::new(),
            tables: HashMap::new(),
        }),
    };
    let cmdbar = CmdCreateDatabase {
        db_name: "bar".into(),
        db: Some(Db {
            db_id: -1,
            ver: -1,
            table_name_to_id: HashMap::new(),
            tables: HashMap::new(),
        }),
    };

    {
        // create db foo
        let rst = eng.create_database(cmdfoo.clone(), false);
        assert_eq!(0, rst.unwrap());
        assert_eq!(
            Db {
                db_id: 0,
                ver: 0,
                table_name_to_id: HashMap::new(),
                tables: HashMap::new(),
            },
            eng.get_database("foo".into()).unwrap()
        );
    }

    {
        // create db bar
        let rst = eng.create_database(cmdbar.clone(), false);
        assert_eq!(1, rst.unwrap());
        assert_eq!(
            Db {
                db_id: 1,
                ver: 1,
                table_name_to_id: HashMap::new(),
                tables: HashMap::new(),
            },
            eng.get_database("bar".into()).unwrap()
        );
    }

    {
        // create db bar failure
        let rst = eng.create_database(cmdbar.clone(), true);
        assert_eq!("database exists", format!("{}", rst.err().unwrap()));
        assert_eq!(
            Db {
                db_id: 1,
                ver: 1,
                table_name_to_id: HashMap::new(),
                tables: HashMap::new(),
            },
            eng.get_database("bar".into()).unwrap(),
            "got the previous bar"
        );
    }
    Ok(())
}
