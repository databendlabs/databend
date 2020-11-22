// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::path::PathBuf;
use std::sync::Arc;

use crate::contexts::FuseQueryContext;
use crate::datasources::{CsvTable, DataSource, Database, IDatabase, Partition};
use crate::datavalues::{DataField, DataSchema, DataSchemaRef, DataType};
use crate::transforms::SourceTransform;

pub struct CsvTestData {
    db: &'static str,
    table: &'static str,
    batch_size: usize,
}

impl CsvTestData {
    pub fn create() -> Self {
        CsvTestData {
            db: "default",
            table: "t1",
            batch_size: 20,
        }
    }

    pub fn csv_table_schema_for_test(&self) -> DataSchemaRef {
        Arc::new(DataSchema::new(vec![
            DataField::new("c1", DataType::Utf8, false),
            DataField::new("c2", DataType::UInt32, false),
            DataField::new("c3", DataType::Int8, false),
            DataField::new("c4", DataType::Int16, false),
            DataField::new("c5", DataType::Int32, false),
            DataField::new("c6", DataType::Int64, false),
            DataField::new("c7", DataType::UInt8, false),
            DataField::new("c8", DataType::UInt16, false),
            DataField::new("c9", DataType::UInt32, false),
            DataField::new("c10", DataType::UInt64, false),
            DataField::new("c11", DataType::Float32, false),
            DataField::new("c12", DataType::Float64, false),
            DataField::new("c13", DataType::Utf8, false),
        ]))
    }

    pub fn csv_table_partitions_for_test(&self) -> Vec<Partition> {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        vec![
            Partition {
                name: format!(
                    "{}/src/testdata/data/csv/part_0000_v0_0000100.csv",
                    dir.display()
                ),
                version: 0,
            },
            Partition {
                name: format!(
                    "{}/src/testdata/data/csv/part_0001_v0_0000000.csv",
                    dir.display()
                ),
                version: 0,
            },
            Partition {
                name: format!(
                    "{}/src/testdata/data/csv/part_0002_v0_0000100.csv",
                    dir.display()
                ),
                version: 0,
            },
            Partition {
                name: format!(
                    "{}/src/testdata/data/csv/part_0003_v0_0000001.csv",
                    dir.display()
                ),
                version: 0,
            },
        ]
    }

    pub fn csv_table_datasource_for_test(&self) -> DataSource {
        let table = CsvTable::create(
            self.table,
            self.batch_size,
            self.csv_table_schema_for_test(),
            self.csv_table_partitions_for_test(),
        );
        let mut database = Database::create(self.db);
        database.add_table(Arc::new(table)).unwrap();
        let mut datasource = DataSource::create();
        datasource.add_database(Arc::new(database)).unwrap();
        datasource
    }

    pub fn csv_table_source_transform_for_test(&self) -> SourceTransform {
        let ctx = FuseQueryContext::create_ctx(0, Arc::new(self.csv_table_datasource_for_test()));
        SourceTransform::create(
            ctx,
            self.db,
            self.table,
            self.csv_table_partitions_for_test(),
        )
    }
}
