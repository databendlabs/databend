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

use databend_common_exception::Result;
use databend_common_expression::Constant;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::Expr;
use databend_common_expression::Scalar;
pub use databend_common_expression::hash_util::hash_by_method;
pub use databend_common_expression::hash_util::hash_by_method_for_bloom;
use databend_common_expression::type_check::check_function;
use databend_common_functions::BUILTIN_FUNCTIONS;

pub(crate) fn build_schema_wrap_nullable(build_schema: &DataSchemaRef) -> DataSchemaRef {
    let mut nullable_field = Vec::with_capacity(build_schema.fields().len());
    for field in build_schema.fields() {
        nullable_field.push(DataField::new(
            field.name(),
            field.data_type().wrap_nullable(),
        ));
    }
    DataSchemaRefExt::create(nullable_field)
}

pub(crate) fn probe_schema_wrap_nullable(probe_schema: &DataSchemaRef) -> DataSchemaRef {
    let mut nullable_field = Vec::with_capacity(probe_schema.fields().len());
    for field in probe_schema.fields() {
        nullable_field.push(DataField::new(
            field.name(),
            field.data_type().wrap_nullable(),
        ));
    }
    DataSchemaRefExt::create(nullable_field)
}

pub(crate) fn min_max_filter(
    min: Scalar,
    max: Scalar,
    probe_key: &Expr<String>,
) -> Result<Expr<String>> {
    let bound_type = probe_key.data_type().remove_nullable();
    let min = Expr::Constant(Constant {
        span: None,
        scalar: min,
        data_type: bound_type.clone(),
    });
    let max = Expr::Constant(Constant {
        span: None,
        scalar: max,
        data_type: bound_type,
    });
    let gte = check_function(
        probe_key.span(),
        "gte",
        &[],
        &[probe_key.clone(), min],
        &BUILTIN_FUNCTIONS,
    )?;
    let lte = check_function(
        probe_key.span(),
        "lte",
        &[],
        &[probe_key.clone(), max],
        &BUILTIN_FUNCTIONS,
    )?;
    check_function(None, "and_filters", &[], &[gte, lte], &BUILTIN_FUNCTIONS)
}
