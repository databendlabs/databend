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
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_ast::ast::Expr as AExpr;
use databend_common_ast::Span;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check::common_super_type;
use databend_common_expression::types::DataType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::Evaluator;
use databend_common_functions::BUILTIN_FUNCTIONS;
use indexmap::IndexMap;

use crate::binder::wrap_cast;
use crate::optimizer::ColumnSet;
use crate::optimizer::SExpr;
use crate::plans::Aggregate;
use crate::plans::AggregateMode;
use crate::plans::BoundColumnRef;
use crate::plans::CacheScan;
use crate::plans::CacheSource;
use crate::plans::ConstantTableScan;
use crate::plans::ExpressionScan;
use crate::plans::HashJoinBuildCacheInfo;
use crate::plans::RelOperator;
use crate::plans::ScalarItem;
use crate::BindContext;
use crate::Binder;
use crate::ColumnBinding;
use crate::ColumnBindingBuilder;
use crate::MetadataRef;
use crate::NameResolutionContext;
use crate::ScalarBinder;
use crate::ScalarExpr;
use crate::Visibility;

// The `ExpressionScanContext` is used to store the information of
// expression scan and hash join build cache.
#[derive(Clone)]
pub struct ExpressionScanContext {
    // Hash join build cache info.
    // Cache column bindings in hash join build side.
    pub cache_columns: Vec<Vec<ColumnBinding>>,
    // Cache column indexes in hash join build side.
    pub column_indexes: Vec<Vec<usize>>,
    // The hash join build cache index which are used in expression scan.
    pub used_cache_indexes: HashSet<usize>,
    // The hash join build cache columns which are used in expression scan.
    pub used_column_indexes: Vec<HashSet<usize>>,

    // Expression scan info.
    // Derived column indexes for each expression scan.
    pub derived_indexes: Vec<HashMap<usize, usize>>,
    // Original column indexes for each expression scan.
    pub originnal_columns: Vec<HashMap<usize, usize>>,
}

impl Default for ExpressionScanContext {
    fn default() -> Self {
        Self::new()
    }
}

impl ExpressionScanContext {
    pub fn new() -> Self {
        ExpressionScanContext {
            cache_columns: vec![],
            column_indexes: vec![],
            used_cache_indexes: HashSet::new(),
            used_column_indexes: vec![],
            derived_indexes: vec![],
            originnal_columns: vec![],
        }
    }

    // Add expression scan and return expression scan index.
    pub fn add_expression_scan(&mut self) -> usize {
        self.derived_indexes.push(HashMap::new());
        self.originnal_columns.push(HashMap::new());
        self.derived_indexes.len() - 1
    }

    pub fn add_expression_scan_column(
        &mut self,
        expression_scan_index: usize,
        original_column_index: usize,
        derived_column_index: usize,
    ) {
        self.derived_indexes[expression_scan_index]
            .insert(original_column_index, derived_column_index);
        self.originnal_columns[expression_scan_index]
            .insert(derived_column_index, original_column_index);
    }

    pub fn get_derived_column(&self, expression_scan_index: usize, column_index: usize) -> usize {
        self.derived_indexes[expression_scan_index]
            .get(&column_index)
            .cloned()
            .unwrap()
    }

    pub fn get_original_column(&self, expression_scan_index: usize, column_index: usize) -> usize {
        self.originnal_columns[expression_scan_index]
            .get(&column_index)
            .cloned()
            .unwrap()
    }

    // Add hash join build cache and return cache index.
    pub fn add_hash_join_build_cache(
        &mut self,
        columns: Vec<ColumnBinding>,
        column_indexes: Vec<usize>,
    ) -> usize {
        self.cache_columns.push(columns);
        self.column_indexes.push(column_indexes);
        self.used_column_indexes.push(HashSet::new());
        self.derived_indexes.push(HashMap::new());
        self.originnal_columns.push(HashMap::new());
        self.cache_columns.len() - 1
    }

    pub fn add_used_cache_index(&mut self, cache_index: usize) {
        self.used_cache_indexes.insert(cache_index);
    }

    pub fn add_used_cache_column_index(&mut self, cache_index: usize, column_index: usize) {
        self.used_column_indexes[cache_index].insert(column_index);
    }

    // Try to find the cache index for the column index.
    pub fn find_cache_index(&self, column_index: usize) -> Option<usize> {
        for idx in (0..self.column_indexes.len()).rev() {
            for index in &self.column_indexes[idx] {
                if *index == column_index {
                    return Some(idx);
                }
            }
        }
        None
    }

    pub fn get_cache_columns(&self, cache_idx: usize) -> &Vec<ColumnBinding> {
        &self.cache_columns[cache_idx]
    }

    pub fn generate_cache_info(&self, cache_idx: usize) -> Option<HashJoinBuildCacheInfo> {
        self.used_cache_indexes.get(&cache_idx)?;
        Some(HashJoinBuildCacheInfo {
            cache_idx,
            columns: self.column_indexes[cache_idx].clone(),
        })
    }
}

impl Binder {
    pub(crate) fn bind_values(
        &mut self,
        bind_context: &mut BindContext,
        span: Span,
        values: &[Vec<AExpr>],
    ) -> Result<(SExpr, BindContext)> {
        bind_values(
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            bind_context,
            span,
            values,
            Some(&mut self.expression_scan_context),
        )
    }

    fn check_values_semantic(span: Span, values: &[Vec<AExpr>]) -> Result<()> {
        if values.is_empty() {
            return Err(ErrorCode::SemanticError(
                "Values lists must have at least one row".to_string(),
            )
            .set_span(span));
        }
        let same_length = values.windows(2).all(|v| v[0].len() == v[1].len());
        if !same_length {
            return Err(ErrorCode::SemanticError(
                "Values lists must all be the same length".to_string(),
            )
            .set_span(span));
        }
        Ok(())
    }

    // Remove unused cache columns and join conditions and construct ExpressionScan's child.
    pub fn construct_expression_scan(
        &mut self,
        s_expr: &SExpr,
        metadata: MetadataRef,
    ) -> Result<(SExpr, ColumnSet)> {
        match s_expr.plan.as_ref() {
            RelOperator::Join(join) if join.build_side_cache_info.is_some() => {
                let mut join = join.clone();
                let build_side_cache_info = join.build_side_cache_info.as_mut().unwrap();

                let (left, left_correlated_columns) =
                    self.construct_expression_scan(s_expr.child(0)?, metadata.clone())?;

                let (right, right_correlated_columns) =
                    self.construct_expression_scan(s_expr.child(1)?, metadata.clone())?;

                // Remove unused cache columns from hash join build side.
                let used_cache_columns = &self.expression_scan_context.used_column_indexes
                    [build_side_cache_info.cache_idx];
                for index in (0..build_side_cache_info.columns.len()).rev() {
                    let column_index = build_side_cache_info.columns[index];
                    if !used_cache_columns.contains(&column_index) {
                        build_side_cache_info.columns.remove(index);
                    }
                }

                // Remove unused join conditions.
                let join_conditions = [join.left_conditions.clone(), join.right_conditions.clone()];
                for index in (0..join.left_conditions.len()).rev() {
                    let mut used = false;
                    for conditions in join_conditions.iter() {
                        let used_columns = conditions[index].used_columns();
                        if used_columns.is_subset(&left_correlated_columns) {
                            used = true;
                            break;
                        }
                    }

                    if !used {
                        join.left_conditions.remove(index);
                        join.right_conditions.remove(index);
                    }
                }

                let s_expr = s_expr
                    .replace_plan(Arc::new(RelOperator::Join(join)))
                    .replace_children(vec![Arc::new(left), Arc::new(right)]);
                Ok((s_expr, right_correlated_columns))
            }
            RelOperator::ExpressionScan(scan) => {
                // The join condition columns may consist of the following two parts:
                // (1) expression scan columns.
                // (2) correlated columns in values.
                let mut join_condition_columns = ColumnSet::new();
                for row in scan.values.iter() {
                    for (scalar, column_index) in row
                        .iter()
                        .zip(scan.column_indexes.iter())
                        .take(scan.num_scalar_columns)
                    {
                        join_condition_columns.insert(*column_index);
                        for index in scalar.used_columns() {
                            let derived_index = self
                                .expression_scan_context
                                .get_derived_column(scan.expression_scan_index, index);
                            join_condition_columns.insert(derived_index);
                        }
                    }
                }

                let mut scan = scan.clone();

                // Remove ExpressionScan unused cache columns.
                let mut cache_scan_columns = vec![];
                let mut cache_scan_column_indexes = vec![];
                for index in (scan.num_scalar_columns..scan.values[0].len()).rev() {
                    let column_index = scan.column_indexes[index];
                    if join_condition_columns.contains(&column_index) {
                        cache_scan_columns.push(scan.values[0][index].clone());
                        let original_index = self
                            .expression_scan_context
                            .get_original_column(scan.expression_scan_index, column_index);
                        cache_scan_column_indexes.push(original_index);
                        self.expression_scan_context
                            .add_used_cache_column_index(scan.cache_index, original_index)
                    } else {
                        scan.remove_cache_column(index);
                    }
                }

                // Construct ExpressionScan schema.
                let mut expression_scan_field = Vec::with_capacity(scan.values[0].len());
                for (column_index, data_type) in
                    scan.column_indexes.iter().zip(scan.data_types.iter())
                {
                    let field = DataField::new(&column_index.to_string(), data_type.clone());
                    expression_scan_field.push(field);
                }
                let expression_scan_schema = DataSchemaRefExt::create(expression_scan_field);
                scan.schema = expression_scan_schema;

                // Construct CacheScan.
                let mut cache_scan_fields = Vec::with_capacity(cache_scan_columns.len());
                for (column, column_index) in cache_scan_columns
                    .iter()
                    .zip(cache_scan_column_indexes.iter())
                {
                    let field = DataField::new(&column_index.to_string(), column.data_type()?);
                    cache_scan_fields.push(field);
                }

                let cache_source = CacheSource::HashJoinBuild((
                    scan.cache_index,
                    cache_scan_column_indexes.clone(),
                ));
                let cache_scan = SExpr::create_leaf(Arc::new(RelOperator::CacheScan(CacheScan {
                    cache_source,
                    columns: ColumnSet::new(),
                    schema: DataSchemaRefExt::create(cache_scan_fields),
                })));

                let mut distinct_columns = Vec::new();
                for column in scan.values[0].iter().skip(scan.num_scalar_columns) {
                    distinct_columns.push(column);
                }

                // Wrap CacheScan with distinct to eliminate duplicates rows.
                let mut group_items = Vec::with_capacity(cache_scan_column_indexes.len());
                for (index, column_index) in cache_scan_column_indexes.iter().enumerate() {
                    group_items.push(ScalarItem {
                        scalar: cache_scan_columns[index].clone(),
                        index: *column_index,
                    });
                }

                let s_expr = SExpr::create_unary(
                    Arc::new(RelOperator::ExpressionScan(scan)),
                    Arc::new(SExpr::create_unary(
                        Arc::new(
                            Aggregate {
                                mode: AggregateMode::Initial,
                                group_items,
                                aggregate_functions: vec![],
                                from_distinct: false,
                                limit: None,
                                grouping_sets: None,
                            }
                            .into(),
                        ),
                        Arc::new(cache_scan),
                    )),
                );

                Ok((s_expr, join_condition_columns))
            }
            _ => {
                let mut correlated_columns = ColumnSet::new();
                let mut children = Vec::with_capacity(s_expr.arity());
                for child in s_expr.children() {
                    let (child, columns) =
                        self.construct_expression_scan(child, metadata.clone())?;
                    children.push(Arc::new(child));
                    correlated_columns.extend(columns);
                }
                Ok((s_expr.replace_children(children), correlated_columns))
            }
        }
    }
}

pub fn bind_values(
    ctx: Arc<dyn TableContext>,
    name_resolution_ctx: &NameResolutionContext,
    metadata: MetadataRef,
    bind_context: &mut BindContext,
    span: Span,
    values: &[Vec<AExpr>],
    expression_scan_info: Option<&mut ExpressionScanContext>,
) -> Result<(SExpr, BindContext)> {
    // Check the semantic of values lists.
    Binder::check_values_semantic(span, values)?;

    let mut scalar_binder = ScalarBinder::new(
        bind_context,
        ctx.clone(),
        name_resolution_ctx,
        metadata.clone(),
        &[],
        HashMap::new(),
        Box::new(IndexMap::new()),
    );

    let num_values = values.len();
    let num_columns = values[0].len();
    // Scalar expressions for each column.
    let mut column_scalars = vec![Vec::with_capacity(num_values); num_columns];
    // Common data type for each column.
    let mut column_common_type = Vec::with_capacity(num_columns);
    // Used cache indexes, if it's not empty, we will use ExpressionScan.
    let mut cache_indexes = HashSet::new();

    for (row_idx, row) in values.iter().enumerate() {
        for (column_idx, expr) in row.iter().enumerate() {
            let (scalar, data_type) = scalar_binder.bind(expr)?;
            let used_columns = scalar.used_columns();
            if !used_columns.is_empty() {
                if let Some(expression_scan_info) = expression_scan_info.as_ref() {
                    for column_index in used_columns {
                        if let Some(cache_index) =
                            expression_scan_info.find_cache_index(column_index)
                        {
                            cache_indexes.insert(cache_index);
                        } else {
                            return Err(ErrorCode::SemanticError(format!(
                                "Can find cache index for {:?}",
                                &scalar
                            ))
                            .set_span(span));
                        }
                    }
                } else {
                    return Err(ErrorCode::SemanticError(format!(
                        "There is no HashJoinBuildSideInfo for column reference {:?}",
                        &scalar
                    ))
                    .set_span(span));
                }
            }
            column_scalars[column_idx].push((scalar, data_type.clone()));

            // Get the common data type for each column.
            if row_idx == 0 {
                column_common_type.push(data_type);
            } else {
                let common_type = &column_common_type[column_idx];
                if common_type != &data_type {
                    match common_super_type(
                        common_type.clone(),
                        data_type.clone(),
                        &BUILTIN_FUNCTIONS.default_cast_rules,
                    ) {
                        Some(new_common_type) => column_common_type[column_idx] = new_common_type,
                        None => {
                            return Err(ErrorCode::SemanticError(format!(
                                "{} and {} don't have common data type",
                                common_type, data_type
                            ))
                            .set_span(span));
                        }
                    }
                }
            }
        }
    }

    if !cache_indexes.is_empty() {
        if cache_indexes.len() > 1 {
            return Err(ErrorCode::SemanticError(
                "Values can only reference one cache".to_string(),
            )
            .set_span(span));
        }
        let cache_index = *cache_indexes.iter().next().unwrap();
        let expression_scan_info = expression_scan_info.unwrap();
        bind_expression_scan(
            metadata,
            bind_context,
            span,
            num_values,
            num_columns,
            column_scalars,
            column_common_type,
            cache_index,
            expression_scan_info,
        )
    } else {
        bind_constant_scan(
            ctx,
            metadata,
            bind_context,
            span,
            num_values,
            num_columns,
            column_scalars,
            column_common_type,
        )
    }
}

pub fn bind_expression_scan(
    metadata: MetadataRef,
    bind_context: &mut BindContext,
    span: Span,
    num_values: usize,
    num_columns: usize,
    column_scalars: Vec<Vec<(ScalarExpr, DataType)>>,
    mut column_common_type: Vec<DataType>,
    cache_index: usize,
    expression_scan_info: &mut ExpressionScanContext,
) -> Result<(SExpr, BindContext)> {
    // Add the cache index to the used cache indexes.
    expression_scan_info.add_used_cache_index(cache_index);
    // Get the columns in the cache.
    let cache_columns = expression_scan_info.get_cache_columns(cache_index).clone();
    // Generate expression scan index.
    let expression_scan_index = expression_scan_info.add_expression_scan();
    // The scalars include the values and the cache columns.
    let mut scalars = vec![Vec::with_capacity(num_columns + cache_columns.len()); num_values];
    for (column_idx, column) in column_scalars.iter().enumerate() {
        for (row_idx, (scalar, data_type)) in column.iter().enumerate() {
            let scalar = if data_type != &column_common_type[column_idx] {
                wrap_cast(scalar, &column_common_type[column_idx])
            } else {
                scalar.clone()
            };
            scalars[row_idx].push(scalar);
        }
    }

    // Assigns default column names col0, col1, etc.
    let names = (0..num_columns + cache_columns.len())
        .map(|i| format!("col{}", i))
        .collect::<Vec<_>>();
    let mut expression_scan_fields = Vec::with_capacity(names.len());
    for column in cache_columns.iter() {
        column_common_type.push(*column.data_type.clone());
    }
    // Build expression scan fields.
    for (name, data_type) in names.into_iter().zip(column_common_type.iter()) {
        let value_field = DataField::new(&name, data_type.clone());
        expression_scan_fields.push(value_field);
    }

    let mut metadata = metadata.write();
    // Column index for the each column.
    let mut column_indexes = Vec::with_capacity(num_columns + cache_columns.len());
    // Add column bindings for expression scan columns.
    for (idx, field) in expression_scan_fields.iter().take(num_columns).enumerate() {
        let index = metadata.columns().len();
        let column_binding = ColumnBindingBuilder::new(
            format!("expr_scan_{}", idx),
            index,
            Box::new(field.data_type().clone()),
            Visibility::Visible,
        )
        .build();
        let _ = metadata.add_derived_column(
            field.name().clone(),
            field.data_type().clone(),
            Some(ScalarExpr::BoundColumnRef(BoundColumnRef {
                span,
                column: column_binding.clone(),
            })),
        );
        bind_context.add_column_binding(column_binding);
        column_indexes.push(index);
    }

    // Add column bindings for cache columns.
    for (idx, cache_column) in cache_columns.iter().enumerate() {
        for row_scalars in scalars.iter_mut() {
            let scalar = ScalarExpr::BoundColumnRef(BoundColumnRef {
                span: None,
                column: cache_column.clone(),
            });
            row_scalars.push(scalar);
        }

        let column_entry = metadata.column(cache_column.index);
        let name = column_entry.name();
        let data_type = column_entry.data_type();
        let new_column_index = metadata.columns().len();
        let new_column_binding = ColumnBindingBuilder::new(
            format!("expr_scan_{}", idx + num_columns),
            new_column_index,
            Box::new(data_type.clone()),
            Visibility::Visible,
        )
        .build();

        let _ = metadata.add_derived_column(
            name,
            data_type,
            Some(ScalarExpr::BoundColumnRef(BoundColumnRef {
                span,
                column: new_column_binding.clone(),
            })),
        );
        bind_context.add_column_binding(new_column_binding);
        column_indexes.push(new_column_index);

        // Record the mapping between original index (cache index in hash join build side) and derived index.
        expression_scan_info.add_expression_scan_column(
            expression_scan_index,
            cache_column.index,
            new_column_index,
        );
    }

    let s_expr = SExpr::create_leaf(Arc::new(
        ExpressionScan {
            expression_scan_index,
            values: scalars,
            num_scalar_columns: num_columns,
            cache_index,
            data_types: column_common_type,
            column_indexes,
            schema: DataSchemaRefExt::create(vec![]),
        }
        .into(),
    ));

    Ok((s_expr, bind_context.clone()))
}

pub fn bind_constant_scan(
    ctx: Arc<dyn TableContext>,
    metadata: MetadataRef,
    bind_context: &mut BindContext,
    span: Span,
    num_values: usize,
    num_columns: usize,
    column_scalars: Vec<Vec<(ScalarExpr, DataType)>>,
    column_common_type: Vec<DataType>,
) -> Result<(SExpr, BindContext)> {
    let names = (0..num_columns)
        .map(|i| format!("col{}", i))
        .collect::<Vec<_>>();
    let mut value_fields = Vec::with_capacity(names.len());
    for (name, common_type) in names.into_iter().zip(column_common_type.into_iter()) {
        let value_field = DataField::new(&name, common_type);
        value_fields.push(value_field);
    }
    let value_schema = DataSchema::new(value_fields);

    let input = DataBlock::empty();
    let func_ctx = ctx.get_function_context()?;
    let evaluator = Evaluator::new(&input, &func_ctx, &BUILTIN_FUNCTIONS);

    // use values to build columns
    let mut value_columns = Vec::with_capacity(column_scalars.len());
    for (scalars, value_field) in column_scalars.iter().zip(value_schema.fields().iter()) {
        let mut builder =
            ColumnBuilder::with_capacity(value_field.data_type(), column_scalars.len());
        for (scalar, value_type) in scalars {
            let scalar = if value_type != value_field.data_type() {
                wrap_cast(scalar, value_field.data_type())
            } else {
                scalar.clone()
            };
            let expr = scalar
                .as_expr()?
                .project_column_ref(|col| value_schema.index_of(&col.index.to_string()).unwrap());
            let result = evaluator.run(&expr)?;

            match result.as_scalar() {
                Some(val) => {
                    builder.push(val.as_ref());
                }
                None => {
                    return Err(ErrorCode::SemanticError(format!(
                        "Value must be a scalar, but get {}",
                        result
                    ))
                    .set_span(span));
                }
            }
        }
        value_columns.push(builder.build());
    }

    // add column bindings
    let mut columns = ColumnSet::new();
    let mut fields = Vec::with_capacity(num_values);
    let mut metadata = metadata.write();
    for value_field in value_schema.fields() {
        let index = metadata.columns().len();
        columns.insert(index);
        let column_binding = ColumnBindingBuilder::new(
            value_field.name().clone(),
            index,
            Box::new(value_field.data_type().clone()),
            Visibility::Visible,
        )
        .build();
        let _ = metadata.add_derived_column(
            value_field.name().clone(),
            value_field.data_type().clone(),
            Some(ScalarExpr::BoundColumnRef(BoundColumnRef {
                span,
                column: column_binding.clone(),
            })),
        );
        bind_context.add_column_binding(column_binding);

        let field = DataField::new(&index.to_string(), value_field.data_type().clone());
        fields.push(field);
    }
    let schema = DataSchemaRefExt::create(fields);

    let s_expr = SExpr::create_leaf(Arc::new(
        ConstantTableScan {
            values: value_columns,
            num_rows: num_values,
            schema,
            columns,
        }
        .into(),
    ));

    Ok((s_expr, bind_context.clone()))
}
