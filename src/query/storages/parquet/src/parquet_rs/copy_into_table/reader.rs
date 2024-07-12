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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::Expr;
use databend_common_expression::RemoteExpr;
use databend_common_expression::TableSchemaRef;
use databend_common_meta_app::principal::NullAs;
use databend_common_storage::parquet_rs::infer_schema_with_extension;
use databend_storages_common_stage::project_columnar;
use opendal::Operator;
use parquet::file::metadata::FileMetaData;

use crate::parquet_rs::parquet_reader::policy::ReadPolicyBuilder;
use crate::parquet_rs::parquet_reader::policy::ReadPolicyImpl;
use crate::parquet_rs::schema::arrow_to_table_schema;
use crate::InMemoryRowGroup;
use crate::ParquetRSReaderBuilder;
use crate::ParquetRSRowGroupPart;
use crate::ReadSettings;

pub struct RowGroupReaderForCopy {
    row_group_reader_builder: Box<dyn ReadPolicyBuilder>,
    output_projection: Vec<Expr>,
}

impl RowGroupReaderForCopy {
    pub async fn build_reader(
        &self,
        part: &ParquetRSRowGroupPart,
        op: Operator,
        read_settings: &ReadSettings,
        batch_size: usize,
    ) -> Result<Option<ReadPolicyImpl>> {
        let row_group = InMemoryRowGroup::new(
            &part.location,
            op.clone(),
            &part.meta,
            None,
            read_settings.max_gap_size,
            read_settings.max_range_size,
        );
        let mut _sorter = None;
        self.row_group_reader_builder
            .build(row_group, None, &mut _sorter, batch_size)
            .await
    }

    pub fn output_projection(&self) -> &[Expr] {
        &self.output_projection
    }

    pub fn try_create(
        location: &str,
        ctx: Arc<dyn TableContext>,
        op: Operator,
        file_metadata: &FileMetaData,
        output_schema: TableSchemaRef,
        default_values: Option<Vec<RemoteExpr>>,
        missing_as: &NullAs,
    ) -> Result<RowGroupReaderForCopy> {
        let arrow_schema = infer_schema_with_extension(file_metadata)?;
        let schema_descr = file_metadata.schema_descr_ptr();
        let parquet_table_schema = Arc::new(arrow_to_table_schema(&arrow_schema)?);

        let (mut output_projection, mut pushdown_columns) = project_columnar(
            &parquet_table_schema,
            &output_schema,
            missing_as,
            &default_values,
            location,
        )?;
        pushdown_columns.sort();
        let mapping = pushdown_columns
            .clone()
            .into_iter()
            .enumerate()
            .map(|(i, pos)| (pos, i))
            .collect::<HashMap<_, _>>();
        for expr in output_projection.iter_mut() {
            match expr {
                Expr::ColumnRef { id, .. } => *id = mapping[id],
                Expr::Cast {
                    expr: box Expr::ColumnRef { id, .. },
                    ..
                } => *id = mapping[id],
                _ => {}
            }
        }
        let pushdowns = PushDownInfo {
            projection: Some(Projection::Columns(pushdown_columns)),
            ..Default::default()
        };
        let mut reader_builder = ParquetRSReaderBuilder::create_with_parquet_schema(
            ctx,
            op,
            parquet_table_schema,
            schema_descr,
            Some(arrow_schema),
        )
        .with_push_downs(Some(&pushdowns));
        reader_builder.build_output()?;

        let row_group_reader_builder = reader_builder.create_no_prefetch_policy_builder()?;
        let reader = RowGroupReaderForCopy {
            row_group_reader_builder,
            output_projection,
        };
        Ok(reader)
    }
}
