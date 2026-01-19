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

use databend_common_catalog::catalog_kind::CATALOG_DEFAULT;
use databend_common_exception::Result;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::expr::*;
use databend_common_expression::filter_helper::FilterHelpers;
use databend_common_expression::type_check::check_string;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::CatalogMeta;
use databend_common_meta_app::schema::CatalogOption;
use databend_common_meta_app::schema::IcebergCatalogOption;
use databend_common_meta_app::schema::IcebergRestCatalogOption;

pub fn generate_catalog_meta(ctl_name: &str) -> CatalogMeta {
    if ctl_name.to_lowercase() == CATALOG_DEFAULT {
        CatalogMeta {
            catalog_option: CatalogOption::Default,
            created_on: Default::default(),
        }
    } else {
        CatalogMeta {
            catalog_option: CatalogOption::Iceberg(IcebergCatalogOption::Rest(
                IcebergRestCatalogOption {
                    uri: "".to_string(),
                    warehouse: "".to_string(),
                    props: Default::default(),
                },
            )),
            created_on: Default::default(),
        }
    }
}

/// Check if current catalog should be included based on filter.
#[inline]
pub fn should_include_catalog(filter_catalog_names: &[String], current_catalog: &str) -> bool {
    filter_catalog_names.is_empty()
        || filter_catalog_names.iter().any(|name| name == current_catalog)
}

/// Check if database-level optimized path should be used.
#[inline]
pub fn should_use_db_optimized_path(
    db_count: usize,
    threshold: usize,
    with_history: bool,
    is_external: bool,
) -> bool {
    db_count > 0 && db_count <= threshold && !with_history && !is_external
}

/// Check if table-level optimized path should be used.
#[inline]
pub fn should_use_table_optimized_path(
    db_count: usize,
    table_count: usize,
    threshold: usize,
    with_history: bool,
    is_external: bool,
) -> bool {
    db_count > 0
        && table_count > 0
        && db_count * table_count <= threshold
        && !with_history
        && !is_external
}

pub fn find_gt_filter(expr: &Expr<String>, visitor: &mut impl FnMut(&str, &Scalar)) {
    match expr {
        Expr::Constant(_) | Expr::ColumnRef(_) => {}
        Expr::Cast(Cast { expr, .. }) => find_gt_filter(expr, visitor),
        Expr::FunctionCall(FunctionCall { function, args, .. }) => {
            if function.signature.name == "gt" || function.signature.name == "gte" {
                match args.as_slice() {
                    [
                        Expr::ColumnRef(ColumnRef { id, .. }),
                        Expr::Constant(Constant { scalar, .. }),
                    ]
                    | [
                        Expr::Constant(Constant { scalar, .. }),
                        Expr::ColumnRef(ColumnRef { id, .. }),
                    ] => {
                        visitor(id, scalar);
                    }
                    _ => {}
                }
            } else if function.signature.name == "and_filters" {
                // only support this:
                // 1. where xx and xx and xx
                // 2. filter: Column `table`, Column `database`
                for arg in args {
                    find_gt_filter(arg, visitor)
                }
            }
        }
        Expr::LambdaFunctionCall(LambdaFunctionCall { args, .. }) => {
            for arg in args {
                find_gt_filter(arg, visitor)
            }
        }
    }
}

pub fn find_lt_filter(expr: &Expr<String>, visitor: &mut impl FnMut(&str, &Scalar)) {
    match expr {
        Expr::Constant(_) | Expr::ColumnRef(_) => {}
        Expr::Cast(Cast { expr, .. }) => find_lt_filter(expr, visitor),
        Expr::FunctionCall(FunctionCall { function, args, .. }) => {
            if function.signature.name == "lt" || function.signature.name == "lte" {
                match args.as_slice() {
                    [
                        Expr::ColumnRef(ColumnRef { id, .. }),
                        Expr::Constant(Constant { scalar, .. }),
                    ]
                    | [
                        Expr::Constant(Constant { scalar, .. }),
                        Expr::ColumnRef(ColumnRef { id, .. }),
                    ] => {
                        visitor(id, scalar);
                    }
                    _ => {}
                }
            } else if function.signature.name == "and_filters" {
                // only support this:
                // 1. where xx and xx and xx
                // 2. filter: Column `table`, Column `database`
                for arg in args {
                    find_lt_filter(arg, visitor)
                }
            }
        }
        Expr::LambdaFunctionCall(LambdaFunctionCall { args, .. }) => {
            for arg in args {
                find_lt_filter(arg, visitor)
            }
        }
    }
}

pub fn extract_leveled_strings(
    expr: &Expr<String>,
    level_names: &[&str],
    func_ctx: &FunctionContext,
) -> Result<(Vec<String>, Vec<String>)> {
    let mut res1 = vec![];
    let mut res2 = vec![];
    let leveld_results =
        FilterHelpers::find_leveled_eq_filters(expr, level_names, func_ctx, &BUILTIN_FUNCTIONS)?;

    for (i, scalars) in leveld_results.iter().enumerate() {
        for r in scalars.iter() {
            let e = Expr::Constant(Constant {
                span: None,
                scalar: r.clone(),
                data_type: r.as_ref().infer_data_type(),
            });

            if let Ok(s) = check_string::<usize>(None, func_ctx, &e, &BUILTIN_FUNCTIONS) {
                match i {
                    0 => res1.push(s),
                    1 => res2.push(s),
                    _ => unreachable!(),
                }
            }
        }
    }
    Ok((res1, res2))
}
