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
    ndv: NdvTrace,
    null_count: u64,
    in_memory_size: u64,
}

#[derive(Debug, Clone)]
enum NdvTrace {
    Estimate(NdvTraceObject),
    /// A scalar or `null` from traces captured before NDV carried an upper.
    Legacy(Option<u64>),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct NdvTraceObject {
    expected: Option<f64>,
    upper: f64,
}

impl serde::Serialize for NdvTrace {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            NdvTrace::Estimate(object) => object.serialize(serializer),
            NdvTrace::Legacy(Some(ndv)) => serializer.serialize_u64(*ndv),
            NdvTrace::Legacy(None) => serializer.serialize_none(),
        }
    }
}

/// Traces are written as an estimate object; older traces hold a scalar or
/// `null`. A
/// visitor is used instead of `#[serde(untagged)]`: with serde_json's
/// `arbitrary_precision`, buffered floats no longer deserialize as `f64`.
impl<'de> serde::Deserialize<'de> for NdvTrace {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = NdvTrace;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str(
                    "an NDV object with `expected` and `upper`, or a legacy integer or null NDV",
                )
            }

            fn visit_u64<E: serde::de::Error>(self, ndv: u64) -> Result<NdvTrace, E> {
                Ok(NdvTrace::Legacy(Some(ndv)))
            }

            fn visit_unit<E: serde::de::Error>(self) -> Result<NdvTrace, E> {
                Ok(NdvTrace::Legacy(None))
            }

            fn visit_none<E: serde::de::Error>(self) -> Result<NdvTrace, E> {
                Ok(NdvTrace::Legacy(None))
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(self, map: A) -> Result<NdvTrace, A::Error> {
                <NdvTraceObject as serde::Deserialize>::deserialize(
                    serde::de::value::MapAccessDeserializer::new(map),
                )
                .map(NdvTrace::Estimate)
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

impl From<&BasicColumnStatistics> for ColumnStatisticsTrace {
    fn from(stats: &BasicColumnStatistics) -> Self {
        let ndv = stats.ndv();
        Self {
            min: stats.min().cloned(),
            max: stats.max().cloned(),
            ndv: NdvTrace::Estimate(NdvTraceObject {
                expected: ndv.expected,
                upper: ndv.upper,
            }),
            null_count: stats.null_count(),
            in_memory_size: stats.in_memory_size(),
        }
    }
}

impl ColumnStatisticsTrace {
    /// Rebuild the provider statistics of a table with `num_rows` rows.
    /// Current traces record provider output, which is replayed unchanged and
    /// must satisfy the provider invariants. Older traces carry no NDV or a
    /// scalar that the planner of that time used as exact, with zero meaning
    /// unknown; they are derived the way providers of that time did.
    pub fn to_basic_column_statistics(
        &self,
        num_rows: Option<u64>,
    ) -> Result<BasicColumnStatistics, ErrorCode> {
        let upper = BasicColumnStatistics::ndv_upper_bound(
            self.min.as_ref(),
            self.max.as_ref(),
            self.null_count,
            num_rows,
        );
        let ndv = match &self.ndv {
            NdvTrace::Estimate(object) => NdvEstimate {
                expected: object.expected,
                upper: object.upper,
            },
            NdvTrace::Legacy(None | Some(0)) => NdvEstimate::upper_bound(upper).bound(upper),
            NdvTrace::Legacy(Some(ndv)) => NdvEstimate::exact(*ndv as f64).bound(upper),
        };
        BasicColumnStatistics::new(
            self.min.clone(),
            self.max.clone(),
            self.null_count,
            self.in_memory_size,
            ndv,
            num_rows,
        )
        .map_err(ErrorCode::InvalidArgument)
    }
}
