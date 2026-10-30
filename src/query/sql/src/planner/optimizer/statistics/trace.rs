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

use databend_common_catalog::BasicColumnStatistics;
use databend_common_exception::ErrorCode;
use databend_common_statistics::Datum;
use databend_common_statistics::NdvEstimate;

/// Wire representation of statistics trace input. Runtime statistics do not
/// implement serde: trace/replay owns this format and its validation boundary.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ColumnStatisticsTrace {
    min: Option<Datum>,
    max: Option<Datum>,
    ndv: Option<NdvTrace>,
    null_count: u64,
    in_memory_size: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
struct NdvTrace {
    expected: Option<f64>,
    upper: f64,
}

#[derive(serde::Deserialize)]
struct NdvTraceObject {
    expected: Option<f64>,
    upper: f64,
}

/// Traces are written as an estimate object. Traces captured before NDV
/// carried an upper bound hold a scalar, which the planner of that time
/// treated as exact; replay keeps that meaning. A visitor is used instead of
/// `#[serde(untagged)]`: with serde_json's `arbitrary_precision`, buffered
/// floats no longer deserialize as `f64`.
impl<'de> serde::Deserialize<'de> for NdvTrace {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = NdvTrace;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an NDV object with `expected` and `upper`, or a legacy integer NDV")
            }

            fn visit_u64<E: serde::de::Error>(self, ndv: u64) -> Result<NdvTrace, E> {
                Ok(NdvTrace {
                    expected: Some(ndv as f64),
                    upper: ndv as f64,
                })
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(self, map: A) -> Result<NdvTrace, A::Error> {
                let object = <NdvTraceObject as serde::Deserialize>::deserialize(
                    serde::de::value::MapAccessDeserializer::new(map),
                )?;
                Ok(NdvTrace {
                    expected: object.expected,
                    upper: object.upper,
                })
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

impl From<&BasicColumnStatistics> for ColumnStatisticsTrace {
    fn from(stats: &BasicColumnStatistics) -> Self {
        Self {
            min: stats.min.clone(),
            max: stats.max.clone(),
            ndv: stats.ndv.map(|ndv| NdvTrace {
                expected: ndv.expected,
                upper: ndv.upper,
            }),
            null_count: stats.null_count,
            in_memory_size: stats.in_memory_size,
        }
    }
}

impl TryFrom<&ColumnStatisticsTrace> for BasicColumnStatistics {
    type Error = ErrorCode;

    fn try_from(stats: &ColumnStatisticsTrace) -> Result<Self, Self::Error> {
        let ndv = stats
            .ndv
            .as_ref()
            .map(|ndv| -> Result<_, ErrorCode> {
                let estimate = NdvEstimate {
                    expected: ndv.expected,
                    upper: ndv.upper,
                };
                estimate
                    .check_consistency()
                    .map_err(ErrorCode::InvalidArgument)?;
                Ok(estimate)
            })
            .transpose()?;
        Ok(Self {
            min: stats.min.clone(),
            max: stats.max.clone(),
            ndv,
            null_count: stats.null_count,
            in_memory_size: stats.in_memory_size,
        })
    }
}
