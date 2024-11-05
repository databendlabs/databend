//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::number::Float64Type;
use databend_common_expression::types::number::Int64Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_query::storages::fuse::statistics::gen_columns_statistics;
use databend_query::test_kits::TestFixture;

fn gen_sample_block() -> (DataBlock, Vec<Column>, TableSchemaRef) {
    //   sample message
    //
    //   struct {
    //      a: struct {
    //          b: struct {
    //             c: i64,
    //             d: f64,
    //          },
    //          e: f64
    //      }
    //      f : i64,
    //      g: f64,
    //   }

    let schema = Arc::new(TableSchema::new(vec![
        TableField::new("a", TableDataType::Tuple {
            fields_name: vec!["b".to_string(), "e".to_string()],
            fields_type: vec![
                TableDataType::Tuple {
                    fields_name: vec!["c".to_string(), "d".to_string()],
                    fields_type: vec![
                        TableDataType::Number(NumberDataType::Int64),
                        TableDataType::Number(NumberDataType::Float64),
                    ],
                },
                TableDataType::Number(NumberDataType::Float64),
            ],
        }),
        TableField::new("f", TableDataType::Number(NumberDataType::Int64)),
        TableField::new("g", TableDataType::Number(NumberDataType::Float64)),
    ]));

    // prepare leaves
    let col_c = Int64Type::from_data(vec![1i64, 2, 3]);
    let col_d = Float64Type::from_data(vec![1.0f64, 2., 3.]);
    let col_e = Float64Type::from_data(vec![4.0f64, 5., 6.]);
    let col_f = Int64Type::from_data(vec![7i64, 8, 9]);
    let col_g = Float64Type::from_data(vec![10.0f64, 11., 12.]);

    // inner/root nodes
    let col_b = Column::Tuple(vec![col_c.clone(), col_d.clone()]);
    let col_a = Column::Tuple(vec![col_b, col_e.clone()]);

    let columns = vec![col_a, col_f.clone(), col_g.clone()];
    (
        DataBlock::new_from_columns(columns),
        vec![col_c, col_d, col_e, col_f, col_g],
        schema,
    )
}

#[test]
fn test_column_statistic() -> Result<()> {
    let (sample_block, sample_cols, schema) = gen_sample_block();
    let col_stats = gen_columns_statistics(&sample_block, None, &schema)?;

    assert_eq!(5, col_stats.len());

    (0..5).for_each(|i| {
        let stats = col_stats.get(&(i as u32)).unwrap();
        let column = &sample_cols[i];
        let values: Vec<Scalar> = (0..column.len())
            .map(|i| column.index(i).unwrap().to_owned())
            .collect();
        assert_eq!(
            stats.min(),
            values.iter().min().unwrap(),
            "checking min of col {}",
            i
        );
        assert_eq!(
            stats.max(),
            values.iter().max().unwrap(),
            "checking max of col {}",
            i
        );
    });

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_accurate_columns_rages() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    // table engines that do not support accurate columns ranges should return None
    {
        fixture
            .execute_command(
                "CREATE table default.test_col_range_in_mem(id int, val string) engine = MEMORY;",
            )
            .await?;

        let catalog = ctx.get_catalog("default").await?;
        let table = catalog
            .get_table(
                &fixture.default_tenant(),
                "default",
                "test_col_range_in_mem",
            )
            .await?;

        let res = table.accurate_columns_ranges(ctx.clone(), &[]).await?;

        // table with memory engine do not support accurate_columns_ranges, expects None
        assert!(res.is_none());
    }

    // Test Fuse engine
    {
        fixture
            .execute_command("CREATE table default.test_accurate_col_range(id int, val string);")
            .await?;

        // 1. Table without a snapshot should return Some(EMPTY_HASH_MAP)

        let catalog = ctx.get_catalog("default").await?;
        let table = catalog
            .get_table(
                &fixture.default_tenant(),
                "default",
                "test_accurate_col_range",
            )
            .await?;
        let res = table.accurate_columns_ranges(ctx.clone(), &[]).await?;

        assert!(res.is_some());
        let res = res.unwrap();
        assert!(res.is_empty());

        // 2. If string-typed column `val` is not trimmed, it should be marked as NOT may_be_truncated

        let catalog = ctx.get_catalog("default").await?;
        fixture
            .execute_command(
                "insert into table default.test_accurate_col_range(id, val) values (1, 'a');",
            )
            .await?;
        let table = catalog
            .get_table(
                &fixture.default_tenant(),
                "default",
                "test_accurate_col_range",
            )
            .await?;
        let res = table
            .accurate_columns_ranges(ctx.clone(), &[0, 1])
            .await?
            .unwrap();
        assert!(!res.get(&0).unwrap().min.may_be_truncated);
        assert!(!res.get(&0).unwrap().max.may_be_truncated);
        assert!(!res.get(&1).unwrap().min.may_be_truncated);
        assert!(!res.get(&1).unwrap().max.may_be_truncated);

        // 3. Insert a long string value, makes the max bound of column `val` trimmed

        let catalog = ctx.get_catalog("default").await?;
        fixture
            .execute_command(
                "insert into table default.test_accurate_col_range(id, val) values (1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');",
            )
            .await?;
        let table = catalog
            .get_table(
                &fixture.default_tenant(),
                "default",
                "test_accurate_col_range",
            )
            .await?;
        let res = table.accurate_columns_ranges(ctx, &[0, 1]).await?.unwrap();
        assert!(!res.get(&0).unwrap().min.may_be_truncated);
        assert!(!res.get(&0).unwrap().max.may_be_truncated);
        assert!(!res.get(&1).unwrap().min.may_be_truncated);
        // max of col `val` should be truncated
        assert!(res.get(&1).unwrap().max.may_be_truncated);
    }

    Ok(())
}
