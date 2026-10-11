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

use databend_common_ast::parser::Dialect;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::FunctionContext;
use databend_common_expression::aggregate_function::AggregateRegistry;
use databend_common_functions::aggregates::AGGR_REGISTRY;
use databend_common_settings::Settings;

use super::TypeCheckAdapter;
use super::UdfAdapter;

/// Capabilities allowed when validating persisted expressions and storage filters.
///
/// Holds only a snapshot of typing/folding inputs, never `TableContext` or any
/// catalog, authorization, stage, sequence, dictionary, or UDF resolver. New
/// capabilities on `FullTypeCheckAdapter` cannot leak into these expressions.
/// Runtime rebinding continues to use Full for compatibility with existing tables.
#[derive(Clone)]
pub struct PersistedTypeCheckAdapter {
    func_ctx: FunctionContext,
    dialect: Dialect,
    inlist_to_join_threshold: usize,
    max_inlist_to_or: u64,
    enable_decimal_sum_widening: bool,
}

impl PersistedTypeCheckAdapter {
    /// Capture the defining session's folding context for compatibility with
    /// append/recluster, which still evaluate with the execution session's context.
    /// This does not make timezone-sensitive ordinary casts context independent.
    pub fn new(settings: &Settings, func_ctx: FunctionContext) -> Result<Self> {
        Ok(Self {
            func_ctx,
            dialect: settings.get_sql_dialect()?,
            inlist_to_join_threshold: settings.get_inlist_to_join_threshold()?,
            max_inlist_to_or: settings.get_max_inlist_to_or()?,
            enable_decimal_sum_widening: settings.get_enable_decimal_sum_widening()?,
        })
    }
}

/// No definition loader, code loader, server folding, or cloud script capability.
#[derive(Clone)]
pub struct NoUdfAdapter;

impl UdfAdapter for NoUdfAdapter {}

impl TypeCheckAdapter for PersistedTypeCheckAdapter {
    type UdfAdapter = NoUdfAdapter;

    fn function_context(&self) -> Result<FunctionContext> {
        Ok(self.func_ctx.clone())
    }

    fn sql_dialect(&self) -> Result<Dialect> {
        Ok(self.dialect)
    }

    fn inlist_to_join_threshold(&self) -> Result<usize> {
        Ok(self.inlist_to_join_threshold)
    }

    fn max_inlist_to_or(&self) -> Result<u64> {
        Ok(self.max_inlist_to_or)
    }

    fn enable_decimal_sum_widening(&self) -> Result<bool> {
        Ok(self.enable_decimal_sum_widening)
    }

    fn aggregate_function_registry(&self) -> &'static AggregateRegistry {
        &AGGR_REGISTRY
    }

    fn udf_adapter(&self) -> Result<Self::UdfAdapter> {
        Err(ErrorCode::SemanticError(
            "UDFs are not allowed in persisted or storage-level expressions",
        ))
    }
}
