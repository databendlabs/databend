// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
use std::collections::HashMap;

use common_flights::status_err;
use pretty_assertions::assert_eq;

use crate::engine::mem_engine::MemEngine;
use crate::protobuf::CmdCreateDatabase;
use crate::protobuf::CmdCreateTable;
use crate::protobuf::Db;
use crate::protobuf::Table;

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
            tables: HashMap::new()
        })
    };
    let cmdbar = CmdCreateDatabase {
        db_name: "bar".into(),
        db: Some(Db {
            db_id: -1,
            ver: -1,
            table_name_to_id: HashMap::new(),
            tables: HashMap::new()
        })
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
                tables: HashMap::new()
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
                tables: HashMap::new()
            },
            eng.get_database("bar".into()).unwrap()
        );
    }

    {
        // create db bar with if_not_exists=true
        let rst = eng.create_database(cmdbar.clone(), true);
        assert_eq!(1, rst.unwrap());
        assert_eq!(
            Db {
                db_id: 1,
                ver: 1,
                table_name_to_id: HashMap::new(),
                tables: HashMap::new()
            },
            eng.get_database("bar".into()).unwrap()
        );
    }

    {
        // create db bar failure
        let rst = eng.create_database(cmdbar.clone(), false);
        assert_eq!("bar database exists", format!("{}", rst.err().unwrap()));
        assert_eq!(
            Db {
                db_id: 1,
                ver: 1,
                table_name_to_id: HashMap::new(),
                tables: HashMap::new()
            },
            eng.get_database("bar".into()).unwrap(),
            "got the previous bar"
        );
    }
    Ok(())
}

#[test]
fn test_mem_engine_create_get_table() -> anyhow::Result<()> {
    // TODO check generated ver
    let eng = MemEngine::create();

    let mut eng = eng.lock().unwrap();

    let cmdfoo = CmdCreateDatabase {
        db_name: "foo".into(),
        db: Some(Db {
            db_id: -1,
            ver: -1,
            table_name_to_id: HashMap::new(),
            tables: HashMap::new()
        })
    };

    let cmd_table = CmdCreateTable {
        db_name: "foo".into(),
        table_name: "t1".into(),
        table: Some(Table {
            table_id: -1,
            ver: -1,
            schema: vec![1, 2, 3],
            options: maplit::hashmap! {"key".into() => "val".into()},
            placement_policy: vec![1, 2, 3]
        })
    };

    {
        // create db foo
        let rst = eng.create_database(cmdfoo.clone(), false);
        assert_eq!(0, rst.unwrap());
    }

    {
        // create table
        let rst = eng.create_table(cmd_table.clone(), false);
        assert_eq!(1, rst.unwrap());

        // get table t1
        let got = eng.get_table("foo".into(), "t1".into());
        assert!(got.is_ok());
        let got = got.unwrap();

        assert_eq!(
            Table {
                table_id: 1,
                ver: 1,
                schema: vec![1, 2, 3],
                options: maplit::hashmap! {"key".into() => "val".into()},
                placement_policy: vec![1, 2, 3]
            },
            got
        );
    }

    {
        // get table, db not found
        let got = eng.get_table("notfound".into(), "t1".into());
        assert!(got.is_err());
        assert_eq!(
            "status: Some requested entity was not found: database not found: notfound",
            status_err(got.err().unwrap()).to_string()
        );
    }

    {
        // get table, table not found
        let got = eng.get_table("foo".into(), "notfound".into());
        assert!(got.is_err());
        assert_eq!(
            "status: Some requested entity was not found: table not found: notfound",
            status_err(got.err().unwrap()).to_string()
        );
    }

    Ok(())
}
