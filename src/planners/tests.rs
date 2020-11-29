// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[test]
fn test_sql_to_plan() -> crate::error::FuseQueryResult<()> {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::{env, fmt::Write};
    use std::{ffi::OsStr, fs, io};

    use pretty_assertions::assert_eq;

    use crate::contexts::FuseQueryContext;
    use crate::datasources::*;
    use crate::datavalues::*;
    use crate::planners::*;

    fn list_of_testdata_paths(root: &str) -> io::Result<Vec<PathBuf>> {
        let mut result = vec![];

        for path in fs::read_dir(root)? {
            let path = path?.path();
            if let Some("testdata") = path.extension().and_then(OsStr::to_str) {
                result.push(path);
            }
        }
        Ok(result)
    }

    let test_path = format!(
        "{}/src/planners/tests/",
        env::current_dir().unwrap().display()
    );
    let test_files = list_of_testdata_paths(test_path.as_str())?;

    for file in test_files {
        let mut actual = "".to_string();

        let test_name = file.file_stem().unwrap().to_str().unwrap();
        let file_name = format!("{}/{}", file.parent().unwrap().to_str().unwrap(), test_name);
        let expect = fs::read_to_string(format!("{}.result", file_name.clone()))
            .expect(&format!("{}.result", file_name.clone()));
        let txt = fs::read_to_string(format!("{}.testdata", file_name.clone())).unwrap();
        let querys = txt.trim().split(";").map(str::trim);

        for query in querys {
            if query.is_empty() {
                continue;
            }

            let statements = DFParser::parse_sql(query)?;
            let statement = &statements[0];
            let statement_str = format!("{:?}", statement);

            let line = "-".repeat(statement_str.len() + 7);
            writeln!(actual, "{}", line).unwrap();
            writeln!(actual, "Query: {:?}\n", statement).unwrap();

            let schema = DataSchema::new(vec![DataField::new("a", DataType::Int64, false)]);
            let table = MemoryTable::create("t1", Arc::new(schema));
            let datasource = get_datasource("memory://")?;
            datasource.lock()?.add_database("default")?;
            datasource.lock()?.add_table("default", Arc::new(table))?;

            let ctx = FuseQueryContext::create_ctx(0, datasource);
            let plan = Planner::new().build(Arc::new(ctx), &statement);
            match plan {
                Ok(v) => {
                    writeln!(actual, "AST:\n{:#?}\n", statement).unwrap();
                    write!(actual, "Plan:\n{:?}\n", v).unwrap()
                }
                Err(e) => write!(actual, "Error:\n{}\n", e.to_string()).unwrap(),
            }
        }

        // Check.
        if expect != actual {
            println!("{} [fail]", test_name);
            println!("expect:{}", expect);
            println!("actual:{}", actual);
            assert_eq!(expect, actual);
        } else {
            println!("{} [pass]", test_name);
        }
    }
    Ok(())
}
