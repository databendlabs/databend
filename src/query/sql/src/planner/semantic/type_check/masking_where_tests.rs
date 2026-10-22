// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use databend_common_ast::ast::Expr;
use databend_common_ast::parser::Dialect;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_exception::Result;
use databend_common_expression::FunctionContext;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::aggregate_function::AggregateRegistry;
use databend_common_expression::types::DataType;
use databend_common_functions::aggregates::AGGR_REGISTRY;
use databend_common_meta_app::schema::SecurityPolicyColumnMap;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_storages_basic::NullTable;
use databend_common_users::security_policy_cache::CachedSecurityPolicy;
use parking_lot::RwLock;

use super::TypeCheckAdapter;
use super::persisted::NoUdfAdapter;
use crate::BaseTableColumn;
use crate::ColumnEntry;
use crate::Metadata;
use crate::binder::BindContext;
use crate::binder::ColumnBindingBuilder;
use crate::binder::ScalarBinder;
use crate::binder::Visibility;
use crate::format_scalar;
use crate::planner::semantic::NameResolutionContext;
use crate::plans::ScalarExpr;

#[derive(Clone)]
struct MaskingAdapter {
    loads: Arc<AtomicUsize>,
}

impl TypeCheckAdapter for MaskingAdapter {
    type UdfAdapter = NoUdfAdapter;
    fn function_context(&self) -> Result<FunctionContext> {
        Ok(FunctionContext::default())
    }
    fn sql_dialect(&self) -> Result<Dialect> {
        Ok(Dialect::PostgreSQL)
    }
    fn inlist_to_join_threshold(&self) -> Result<usize> {
        Ok(1024)
    }
    fn max_inlist_to_or(&self) -> Result<u64> {
        Ok(3)
    }
    fn enable_decimal_sum_widening(&self) -> Result<bool> {
        Ok(false)
    }
    fn aggregate_function_registry(&self) -> &'static AggregateRegistry {
        &AGGR_REGISTRY
    }
    fn udf_adapter(&self) -> Result<Self::UdfAdapter> {
        Ok(NoUdfAdapter)
    }
    fn resolve_data_mask_policy(&self, _: u64) -> Result<Option<Arc<CachedSecurityPolicy>>> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        Ok(Some(Arc::new(CachedSecurityPolicy {
            expr: ast("object_delete(val, 'secret')")?,
            args: vec![("val".into(), "VARIANT".into())],
        })))
    }
}

fn ast(sql: &str) -> Result<Expr> {
    let tokens = tokenize_sql(sql)?;
    Ok(parse_expr(&tokens, Dialect::PostgreSQL)?)
}

#[test]
fn where_uses_original_only_for_authorized_base_columns() -> Result<()> {
    let metadata = Arc::new(RwLock::new(Metadata::default()));
    // Stable source IDs intentionally differ from query symbols and field positions.
    let table = NullTable::try_create(TableInfo {
        name: "masked".into(),
        meta: TableMeta {
            schema: Arc::new(TableSchema::new_from_column_ids(
                vec![
                    TableField::new_from_column_id("v", TableDataType::Variant, 17),
                    TableField::new_from_column_id("w", TableDataType::Variant, 42),
                ],
                Default::default(),
                43,
            )),
            options: [("enable_virtual_column".into(), "true".into())].into(),
            column_mask_policy_columns_ids: [
                (17, SecurityPolicyColumnMap::new(1, vec![17])),
                (42, SecurityPolicyColumnMap::new(1, vec![42])),
            ]
            .into(),
            masking_where_use_unmasked_value: [17].into(),
            ..Default::default()
        },
        ..Default::default()
    })?;
    let table_index = metadata.write().add_table(
        "default".into(),
        "default".into(),
        table.into(),
        None,
        None,
        false,
        false,
        None,
    );
    let mut context = BindContext::new();
    context.allow_virtual_column = true;
    for column in metadata.read().columns_by_table_index(table_index) {
        if let ColumnEntry::BaseTableColumn(BaseTableColumn {
            column_name,
            column_index,
            data_type,
            ..
        }) = column
        {
            context.add_column_binding(
                ColumnBindingBuilder::new(
                    column_name.clone(),
                    *column_index,
                    Box::new(DataType::from(data_type)),
                    Visibility::Visible,
                )
                .table_index(Some(table_index))
                .table_name(Some("masked".into()))
                .build(),
            );
        }
    }
    let names = NameResolutionContext::default();
    let adapter = MaskingAdapter {
        loads: Arc::new(AtomicUsize::new(0)),
    };
    let resolve = |context: &mut BindContext,
                   sql: &str,
                   is_where: bool,
                   aliases: &[(String, ScalarExpr)],
                   originals: Vec<(String, Expr)>|
     -> Result<ScalarExpr> {
        let mut binder =
            ScalarBinder::with_adapter(context, adapter.clone(), &names, metadata.clone(), aliases);
        if is_where {
            binder.set_masking_where_aliases(originals);
        }
        Ok(binder.bind(&ast(sql)?)?.0)
    };

    let select = resolve(&mut context, "v['secret']", false, &[], vec![])?;
    assert!(format_scalar(&select).contains("object_delete"));
    let loads = adapter.loads.load(Ordering::SeqCst);
    let raw = resolve(&mut context, "v['secret']", true, &[], vec![])?;
    assert!(matches!(raw, ScalarExpr::BoundColumnRef(_)));
    assert_eq!(adapter.loads.load(Ordering::SeqCst), loads);
    let still_masked = resolve(&mut context, "w['secret']", true, &[], vec![])?;
    assert!(format_scalar(&still_masked).contains("object_delete"));
    // A later non-WHERE binder must not inherit the exception.
    let select_again = resolve(&mut context, "v['secret']", false, &[], vec![])?;
    assert!(format_scalar(&select_again).contains("object_delete"));

    let alias = resolve(
        &mut context,
        "email_alias",
        true,
        &[("email_alias".into(), select)],
        vec![("email_alias".into(), ast("v['secret']")?)],
    )?;
    assert_eq!(alias, raw);

    // A registered internal raw path cannot be used as a user-visible SQL column.
    let ScalarExpr::BoundColumnRef(raw_column) = &raw else {
        unreachable!()
    };
    let identifier = raw_column.column.column_name.replace('"', "\"\"");
    assert!(
        resolve(
            &mut context,
            &format!("\"{identifier}\""),
            false,
            &[],
            vec![]
        )
        .is_err()
    );
    Ok(())
}
