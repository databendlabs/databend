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

use databend_common_catalog::runtime_filter_info::RuntimeFilterInfo;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDomain;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::Column;
use databend_common_expression::Constant;
use databend_common_expression::Domain;
use databend_common_expression::Expr;
use databend_common_expression::RawExpr;
use databend_common_expression::Scalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use xorf::BinaryFuse16;

use super::packet::JoinRuntimeFilterPacket;
use super::packet::SerializableDomain;
use crate::pipelines::processors::transforms::hash_join::desc::RuntimeFilterDesc;
use crate::pipelines::processors::transforms::hash_join::util::min_max_filter;

/// # Note
///
/// The key in the resulting [`HashMap`] is the scan_id, which identifies the scan operator
/// where the runtime filter will be applied. This is different from the runtime filter's own id,
pub fn build_runtime_filter_infos(
    packet: JoinRuntimeFilterPacket,
    runtime_filter_descs: HashMap<usize, &RuntimeFilterDesc>,
) -> Result<HashMap<usize, RuntimeFilterInfo>> {
    let Some(packets) = packet.packets else {
        return Ok(HashMap::new());
    };
    let mut filters: HashMap<usize, RuntimeFilterInfo> = HashMap::new();
    for packet in packets.into_values() {
        let desc = runtime_filter_descs.get(&packet.id).unwrap();
        let entry = filters.entry(desc.scan_id).or_default();
        if let Some(inlist) = packet.inlist {
            entry
                .inlist
                .push(build_inlist_filter(inlist, &desc.probe_key)?);
        }
        if let Some(min_max) = packet.min_max {
            entry.min_max.push(build_min_max_filter(
                min_max,
                &desc.probe_key,
                &desc.build_key,
            )?);
        }
        if let Some(bloom) = packet.bloom {
            entry
                .bloom
                .push(build_bloom_filter(bloom, &desc.probe_key)?);
        }
    }
    Ok(filters)
}

fn build_inlist_filter(inlist: Column, probe_key: &Expr<String>) -> Result<Expr<String>> {
    if inlist.len() == 0 {
        return Ok(Expr::Constant(Constant {
            span: None,
            scalar: Scalar::Boolean(false),
            data_type: DataType::Boolean,
        }));
    }
    let probe_key = probe_key.as_column_ref().unwrap();

    let raw_probe_key = RawExpr::ColumnRef {
        span: probe_key.span,
        id: probe_key.id.to_string(),
        data_type: probe_key.data_type.clone(),
        display_name: probe_key.display_name.clone(),
    };

    let scalars: Vec<_> = inlist
        .iter()
        .map(|scalar_ref| scalar_ref.to_owned())
        .collect();

    let balanced_expr = build_balanced_or_tree(&raw_probe_key, &scalars);

    let expr = type_check::check(&balanced_expr, &BUILTIN_FUNCTIONS)?;
    Ok(expr)
}

fn build_balanced_or_tree(probe_key: &RawExpr<String>, scalars: &[Scalar]) -> RawExpr<String> {
    match scalars.len() {
        0 => RawExpr::Constant {
            span: None,
            scalar: Scalar::Boolean(false),
            data_type: None,
        },
        1 => {
            let constant_expr = RawExpr::Constant {
                span: None,
                scalar: scalars[0].clone(),
                data_type: None,
            };
            RawExpr::FunctionCall {
                span: None,
                name: "eq".to_string(),
                params: vec![],
                args: vec![probe_key.clone(), constant_expr],
            }
        }
        _ => {
            let mid = scalars.len() / 2;
            let left_subtree = build_balanced_or_tree(probe_key, &scalars[..mid]);
            let right_subtree = build_balanced_or_tree(probe_key, &scalars[mid..]);

            RawExpr::FunctionCall {
                span: None,
                name: "or".to_string(),
                params: vec![],
                args: vec![left_subtree, right_subtree],
            }
        }
    }
}

fn build_min_max_filter(
    min_max: SerializableDomain,
    probe_key: &Expr<String>,
    build_key: &Expr,
) -> Result<Expr<String>> {
    let min_max = Domain::from_min_max(
        min_max.min,
        min_max.max,
        &build_key.data_type().remove_nullable(),
    );
    let min_max_filter = match min_max {
        Domain::Number(domain) => match domain {
            NumberDomain::UInt8(simple_domain) => {
                let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                min_max_filter(min, max, probe_key)?
            }
            NumberDomain::UInt16(simple_domain) => {
                let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                min_max_filter(min, max, probe_key)?
            }
            NumberDomain::UInt32(simple_domain) => {
                let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                min_max_filter(min, max, probe_key)?
            }
            NumberDomain::UInt64(simple_domain) => {
                let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                min_max_filter(min, max, probe_key)?
            }
            NumberDomain::Int8(simple_domain) => {
                let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                min_max_filter(min, max, probe_key)?
            }
            NumberDomain::Int16(simple_domain) => {
                let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                min_max_filter(min, max, probe_key)?
            }
            NumberDomain::Int32(simple_domain) => {
                let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                min_max_filter(min, max, probe_key)?
            }
            NumberDomain::Int64(simple_domain) => {
                let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                min_max_filter(min, max, probe_key)?
            }
            NumberDomain::Float32(simple_domain) => {
                let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                min_max_filter(min, max, probe_key)?
            }
            NumberDomain::Float64(simple_domain) => {
                let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                min_max_filter(min, max, probe_key)?
            }
        },
        Domain::String(domain) => {
            let min = Scalar::String(domain.min);
            let max = Scalar::String(domain.max.unwrap());
            min_max_filter(min, max, probe_key)?
        }
        Domain::Date(date_domain) => {
            let min = Scalar::Date(date_domain.min);
            let max = Scalar::Date(date_domain.max);
            min_max_filter(min, max, probe_key)?
        }
        _ => {
            return Err(ErrorCode::UnsupportedDataType(format!(
                "Unsupported domain {:?} for runtime filter",
                min_max,
            )))
        }
    };
    Ok(min_max_filter)
}

fn build_bloom_filter(
    bloom: HashSet<u64>,
    probe_key: &Expr<String>,
) -> Result<(String, BinaryFuse16)> {
    let probe_key = probe_key.as_column_ref().unwrap();
    let hashes_vec = bloom.into_iter().collect::<Vec<_>>();
    let filter = BinaryFuse16::try_from(&hashes_vec)?;
    Ok((probe_key.id.to_string(), filter))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::ColumnBuilder;
    use databend_common_expression::ColumnRef;
    use databend_common_expression::Constant;
    use databend_common_expression::ConstantFolder;
    use databend_common_expression::Domain;
    use databend_common_expression::Expr;
    use databend_common_expression::FunctionContext;
    use databend_common_expression::Scalar;
    use databend_common_functions::BUILTIN_FUNCTIONS;

    use super::build_inlist_filter;

    #[test]
    fn test_build_inlist_filter() {
        let func_ctx = FunctionContext::default();

        // Create test column with values {1, 10}
        let data_type = DataType::Number(NumberDataType::Int32);
        let mut builder = ColumnBuilder::with_capacity(&data_type, 2);
        builder.push(Scalar::Number(1i32.into()).as_ref());
        builder.push(Scalar::Number(10i32.into()).as_ref());
        let inlist = builder.build();

        // Create probe key expression: column_a
        let probe_key = Expr::ColumnRef(ColumnRef {
            span: None,
            id: "column_a".to_string(),
            data_type: data_type.clone(),
            display_name: "column_a".to_string(),
        });

        // Build the filter expression
        let filter_expr = build_inlist_filter(inlist, &probe_key).unwrap();

        // Test with ConstantFolder - case where column_a in [2,10] (can be folded to constant)
        let mut input_domains = HashMap::new();
        let domain_value_2_10 = Domain::from_min_max(
            Scalar::Number(2i32.into()),
            Scalar::Number(10i32.into()),
            &data_type,
        );
        input_domains.insert("column_a".to_string(), domain_value_2_10);

        let (folded_expr, _) = ConstantFolder::fold_with_domain(
            &filter_expr,
            &input_domains,
            &func_ctx,
            &BUILTIN_FUNCTIONS,
        );

        // Verify it's not folded to constant
        assert!(folded_expr.as_constant().is_none());

        // Test with ConstantFolder - case where column_a in [2,9] (should evaluate to false)
        let mut input_domains_false = HashMap::new();
        let domain_value_2_9 = Domain::from_min_max(
            Scalar::Number(2i32.into()),
            Scalar::Number(9i32.into()),
            &data_type,
        );
        input_domains_false.insert("column_a".to_string(), domain_value_2_9);

        let (folded_expr_false, _) = ConstantFolder::fold_with_domain(
            &filter_expr,
            &input_domains_false,
            &func_ctx,
            &BUILTIN_FUNCTIONS,
        );

        // Range [2,9] does not intersect with {1, 10}, so it should fold to constant false
        match folded_expr_false {
            Expr::Constant(Constant {
                scalar: Scalar::Boolean(false),
                ..
            }) => {
                println!("✓ Test passed: column_a in [2,9] correctly evaluated to false");
            }
            _ => {
                panic!("Expected constant false, got: {:?}", folded_expr_false);
            }
        }
    }

    #[test]
    fn test_build_inlist_filter_large() {
        let func_ctx = FunctionContext::default();

        // Create test column with 1024 elements: {0, 1, 2, ..., 1023}
        let data_type = DataType::Number(NumberDataType::Int32);
        let mut builder = ColumnBuilder::with_capacity(&data_type, 1024);
        for i in 0..1024 {
            builder.push(Scalar::Number((i).into()).as_ref());
        }
        let inlist = builder.build();

        // Create probe key expression: column_b
        let probe_key = Expr::ColumnRef(ColumnRef {
            span: None,
            id: "column_b".to_string(),
            data_type: data_type.clone(),
            display_name: "column_b".to_string(),
        });

        // Build the filter expression - this should create a balanced binary tree
        let filter_expr = build_inlist_filter(inlist, &probe_key).unwrap();

        // Verify the expression was built successfully
        assert!(
            filter_expr.as_constant().is_none(),
            "Filter expression should not be a constant"
        );

        // Test with ConstantFolder - case where column_b in [500, 600]
        // (should intersect with our range [0, 1023])
        let mut input_domains = HashMap::new();
        let domain_value_500_600 = Domain::from_min_max(
            Scalar::Number(500i32.into()),
            Scalar::Number(600i32.into()),
            &data_type,
        );
        input_domains.insert("column_b".to_string(), domain_value_500_600);

        let (folded_expr, _) = ConstantFolder::fold_with_domain(
            &filter_expr,
            &input_domains,
            &func_ctx,
            &BUILTIN_FUNCTIONS,
        );

        // Should not fold to constant since there's intersection
        assert!(
            folded_expr.as_constant().is_none(),
            "Expression should not fold to constant when there's intersection"
        );

        // Test with ConstantFolder - case where column_b in [2000, 3000]
        // (should NOT intersect with our range [0, 1023])
        let mut input_domains_no_intersect = HashMap::new();
        let domain_value_2000_3000 = Domain::from_min_max(
            Scalar::Number(2000i32.into()),
            Scalar::Number(3000i32.into()),
            &data_type,
        );
        input_domains_no_intersect.insert("column_b".to_string(), domain_value_2000_3000);

        let (folded_expr_false, _) = ConstantFolder::fold_with_domain(
            &filter_expr,
            &input_domains_no_intersect,
            &func_ctx,
            &BUILTIN_FUNCTIONS,
        );

        // Range [2000, 3000] does not intersect with {0, 1, 2, ..., 1023},
        // so it should fold to constant false
        match folded_expr_false {
            Expr::Constant(Constant {
                scalar: Scalar::Boolean(false),
                ..
            }) => {
                println!("✓ Test passed: column_b in [2000,3000] correctly evaluated to false for 1024 elements");
            }
            _ => {
                panic!(
                    "Expected constant false for non-intersecting range, got: {:?}",
                    folded_expr_false
                );
            }
        }

        println!("✓ Large inlist filter test (1024 elements) passed - balanced binary tree working correctly");
    }
}
