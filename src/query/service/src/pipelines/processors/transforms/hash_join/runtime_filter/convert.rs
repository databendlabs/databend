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
use databend_common_expression::types::NumberDomain;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::Column;
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
    let probe_key = probe_key.as_column_ref().unwrap();

    let raw_probe_key = RawExpr::ColumnRef {
        span: probe_key.span,
        id: probe_key.id.to_string(),
        data_type: probe_key.data_type.clone(),
        display_name: probe_key.display_name.clone(),
    };
    let array = RawExpr::Constant {
        span: None,
        scalar: Scalar::Array(inlist),
        data_type: None,
    };

    let args = vec![array, raw_probe_key];
    let contain_func = RawExpr::FunctionCall {
        span: None,
        name: "contains".to_string(),
        params: vec![],
        args,
    };
    let expr = type_check::check(&contain_func, &BUILTIN_FUNCTIONS)?;
    Ok(expr)
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
