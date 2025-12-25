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

use std::fmt::Display;

use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;

use super::AggregateFunctionDesc;
use crate::IndexType;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum WindowFunction {
    Aggregate(AggregateFunctionDesc),
    RowNumber,
    Rank,
    DenseRank,
    PercentRank,
    LagLead(LagLeadFunctionDesc),
    NthValue(NthValueFunctionDesc),
    Ntile(NtileFunctionDesc),
    CumeDist,
}

impl WindowFunction {
    pub fn data_type(&self) -> Result<DataType> {
        let return_type = match self {
            WindowFunction::Aggregate(agg) => agg.sig.return_type.clone(),
            WindowFunction::RowNumber | WindowFunction::Rank | WindowFunction::DenseRank => {
                DataType::Number(NumberDataType::UInt64)
            }
            WindowFunction::PercentRank | WindowFunction::CumeDist => {
                DataType::Number(NumberDataType::Float64)
            }
            WindowFunction::LagLead(f) => f.return_type.clone(),
            WindowFunction::NthValue(f) => f.return_type.clone(),
            WindowFunction::Ntile(f) => f.return_type.clone(),
        };
        Ok(return_type)
    }
}

impl Display for WindowFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            WindowFunction::Aggregate(agg) => write!(f, "{}", agg.sig.name),
            WindowFunction::RowNumber => write!(f, "row_number"),
            WindowFunction::Rank => write!(f, "rank"),
            WindowFunction::DenseRank => write!(f, "dense_rank"),
            WindowFunction::PercentRank => write!(f, "percent_rank"),
            WindowFunction::LagLead(lag_lead) if lag_lead.is_lag => write!(f, "lag"),
            WindowFunction::LagLead(_) => write!(f, "lead"),
            WindowFunction::NthValue(_) => write!(f, "nth_value"),
            WindowFunction::Ntile(_) => write!(f, "ntile"),
            WindowFunction::CumeDist => write!(f, "cume_dist"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum LagLeadDefault {
    Null,
    Index(IndexType),
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LagLeadFunctionDesc {
    pub is_lag: bool,
    pub offset: u64,
    pub arg: usize,
    pub return_type: DataType,
    pub default: LagLeadDefault,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NthValueFunctionDesc {
    pub n: Option<u64>,
    pub arg: usize,
    pub return_type: DataType,
    pub ignore_null: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NtileFunctionDesc {
    pub n: u64,
    pub return_type: DataType,
}
