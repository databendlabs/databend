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

use std::sync::Arc;
use std::sync::OnceLock;

use databend_common_ast::ast::Expr;
use databend_common_ast::parser::Dialect;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_catalog::table_context::TableContext;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_sql::BindContext;
use databend_common_sql::ColumnBindingBuilder;
use databend_common_sql::Metadata;
use databend_common_sql::MetadataRef;
use databend_common_sql::NameResolutionContext;
use databend_common_sql::TypeChecker;
use databend_common_sql::Visibility;
use databend_common_sql_test_support::LiteTableContext;
use parking_lot::RwLock;
use tokio::runtime::Runtime;

fn main() {
    divan::main();
}

struct TypeCheckCase {
    asts: Vec<Expr>,
    bind_context: BindContext,
    ctx: Arc<dyn TableContext>,
    name_resolution_ctx: NameResolutionContext,
    metadata: MetadataRef,
}

impl TypeCheckCase {
    fn new(sqls: Vec<String>) -> Self {
        let asts = sqls.iter().map(|sql| parse_type_check_expr(sql)).collect();
        let ctx = lite_context();
        let name_resolution_ctx =
            NameResolutionContext::try_from(ctx.get_settings().as_ref()).unwrap();
        let (bind_context, metadata) = build_bind_context(&[
            ("inner_group_id", DataType::Number(NumberDataType::Int64)),
            (
                "inner_mod_group_id",
                DataType::Number(NumberDataType::Int64),
            ),
            (
                "media_source",
                DataType::Nullable(Box::new(DataType::String)),
            ),
            (
                "af_cohort_install_agg",
                DataType::Number(NumberDataType::Float64),
            ),
            ("c_cac_d0", DataType::Number(NumberDataType::Float64)),
            ("c_roi_d0", DataType::Number(NumberDataType::Float64)),
            ("c_roas_d0", DataType::Number(NumberDataType::Float64)),
        ]);

        TypeCheckCase {
            asts,
            bind_context,
            ctx,
            name_resolution_ctx,
            metadata,
        }
    }

    fn resolve_all(&self, bind_context: &mut BindContext) -> usize {
        bench_runtime().block_on(async {
            let mut type_checker = TypeChecker::try_create(
                bind_context,
                self.ctx.clone(),
                &self.name_resolution_ctx,
                self.metadata.clone(),
                &[],
                true,
            )
            .unwrap();
            let mut resolved = 0;
            for ast in &self.asts {
                let (scalar, _) = *type_checker.resolve(ast).unwrap();
                divan::black_box(scalar);
                resolved += 1;
            }
            resolved
        })
    }
}

fn bench_runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();

    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap()
    })
}

fn lite_context() -> Arc<dyn TableContext> {
    static CONTEXT: OnceLock<Arc<LiteTableContext>> = OnceLock::new();

    CONTEXT
        .get_or_init(|| {
            bench_runtime()
                .block_on(LiteTableContext::create())
                .unwrap()
        })
        .clone()
}

fn parse_type_check_expr(sql: &str) -> Expr {
    let tokens = tokenize_sql(sql).unwrap();
    parse_expr(&tokens, Dialect::PostgreSQL).unwrap()
}

fn build_bind_context(columns: &[(&str, DataType)]) -> (BindContext, MetadataRef) {
    let mut bind_context = BindContext::new();
    let mut metadata = Metadata::default();

    for (name, data_type) in columns {
        let column_index = metadata.add_derived_column((*name).to_string(), data_type.clone());
        let column = ColumnBindingBuilder::new(
            (*name).to_string(),
            column_index,
            Box::new(data_type.clone()),
            Visibility::Visible,
        )
        .build();
        bind_context.add_column_binding(column);
    }

    (bind_context, Arc::new(RwLock::new(metadata)))
}

fn rewritten_original_partition_key() -> String {
    "concat(\
        if(\
            bit_and(inner_mod_group_id, 2) = 0,\
            cast(media_source as varchar),\
            ''\
        ),\
        if(\
            inner_mod_group_id = 3,\
            '1',\
            concat(\
                if(\
                    media_source is null,\
                    'ALL',\
                    cast(media_source as varchar)\
                )\
            )\
        )\
    )"
    .to_string()
}

fn repeated_rewritten_original_partition_key(nodes: usize) -> String {
    let key = rewritten_original_partition_key();
    let mut args = Vec::with_capacity(nodes);
    for _ in 0..nodes {
        args.push(key.clone());
    }
    format!("concat({})", args.join(", "))
}

fn rewritten_original_window_exprs(nodes: usize) -> Vec<String> {
    let key = rewritten_original_partition_key();
    let metrics = [
        ("af_cohort_install_agg", "desc"),
        ("c_cac_d0", "asc"),
        ("c_roi_d0", "desc"),
        ("c_roas_d0", "desc"),
    ];

    let mut exprs = Vec::with_capacity(nodes);
    for idx in 0..nodes {
        let (metric, order) = metrics[idx % metrics.len()];
        exprs.push(format!(
            "1 - PERCENT_RANK() OVER (\
                PARTITION BY inner_group_id, {key} \
                ORDER BY {metric} {order}\
            )"
        ));
    }
    exprs
}

#[divan::bench_group(max_time = 1)]
mod resolve {
    use super::*;

    #[divan::bench(args = [8, 32])]
    fn rewritten_original_windows(bencher: divan::Bencher, nodes: usize) {
        let case = TypeCheckCase::new(rewritten_original_window_exprs(nodes));
        let mut bind_context = case.bind_context.clone();

        bencher.bench_local(|| {
            let resolved = case.resolve_all(divan::black_box(&mut bind_context));
            divan::black_box(resolved);
        });
    }

    #[divan::bench(args = [16, 64])]
    fn rewritten_original_partition_keys(bencher: divan::Bencher, nodes: usize) {
        let case = TypeCheckCase::new(vec![repeated_rewritten_original_partition_key(nodes)]);
        let mut bind_context = case.bind_context.clone();

        bencher.bench_local(|| {
            let resolved = case.resolve_all(divan::black_box(&mut bind_context));
            divan::black_box(resolved);
        });
    }
}
