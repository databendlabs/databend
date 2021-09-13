// Copyright 2020 Datafuse Labs.
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

use common_exception::Result;
use common_planners::PlanNode;
use pretty_assertions::assert_eq;

use crate::catalogs::MetaBackend;
use crate::datasources::database::local::LocalMetaBackend;
use crate::sql::PlanParser;

#[test]
fn test_meta_backend() -> Result<()> {
    let ctx = crate::tests::try_create_context()?;
    let backend = LocalMetaBackend::create();

    if let PlanNode::CreateDatabase(plan) =
        PlanParser::create(ctx.clone()).build_from_sql("create database default")?
    {
        backend.create_database(plan)?;
    } else {
        assert!(true);
    }

    if let PlanNode::CreateTable(plan) = PlanParser::create(ctx.clone())
        .build_from_sql("create table default.a(a bigint, b int, c varchar(255), d smallint, e Date ) Engine = Null")?
    {
        backend.create_table(plan)?;
        let table = backend.get_table("default", "a")?;
        assert_eq!("a", table.raw().name());

        let exists = backend.exists_database("default")?;
        assert_eq!(true, exists);

        let exists = backend.exists_database("defaultx")?;
        assert_eq!(false, exists);

        let dbs = backend.get_databases()?;
        assert_eq!(1, dbs.len());

        let tables = backend.get_tables("default")?;
        assert_eq!(1, tables.len());

    } else {
        assert!(true);
    }

    if let PlanNode::DropTable(plan) =
        PlanParser::create(ctx.clone()).build_from_sql("drop table default.a")?
    {
        backend.drop_table(plan)?;
        let table = backend.get_table("default", "a");
        assert_eq!(true, table.is_err());
    } else {
        assert!(true);
    }

    if let PlanNode::DropDatabase(plan) =
        PlanParser::create(ctx).build_from_sql("drop database default")?
    {
        backend.drop_database(plan)?;
        let database = backend.get_database("default");
        assert_eq!(true, database.is_err());
    } else {
        assert!(true);
    }

    Ok(())
}
