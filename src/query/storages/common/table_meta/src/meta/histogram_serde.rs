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

use databend_common_expression::ColumnId;
use databend_common_statistics::Datum;
use databend_common_statistics::Histogram;
use databend_common_statistics::HistogramBucket;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

pub(crate) fn serialize<S>(
    histograms: &HashMap<ColumnId, Histogram>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    histograms
        .iter()
        .map(|(column_id, histogram)| (*column_id, LegacyHistogram::from(histogram)))
        .collect::<HashMap<_, _>>()
        .serialize(serializer)
}

pub(crate) fn deserialize<'de, D>(
    deserializer: D,
) -> Result<HashMap<ColumnId, Histogram>, D::Error>
where D: Deserializer<'de> {
    let raw = HashMap::<ColumnId, serde_json::Value>::deserialize(deserializer)?;
    Ok(raw
        .into_iter()
        .filter_map(|(column_id, value)| parse_histogram(value).map(|hist| (column_id, hist)))
        .collect())
}

fn parse_histogram(value: serde_json::Value) -> Option<Histogram> {
    serde_json::from_value::<LegacyHistogram>(value)
        .ok()
        .and_then(|histogram| histogram.try_into().ok())
}

#[derive(Serialize, Deserialize)]
struct LegacyHistogram {
    accuracy: bool,
    buckets: Vec<LegacyHistogramBucket>,
    #[serde(default)]
    avg_spacing: Option<f64>,
}

impl From<&Histogram> for LegacyHistogram {
    fn from(histogram: &Histogram) -> Self {
        Self {
            accuracy: histogram.accuracy(),
            buckets: histogram
                .bucket_iter()
                .map(|bucket| LegacyHistogramBucket::from(bucket.owned()))
                .collect(),
            avg_spacing: histogram.avg_spacing(),
        }
    }
}

impl TryFrom<LegacyHistogram> for Histogram {
    type Error = &'static str;

    fn try_from(value: LegacyHistogram) -> Result<Self, Self::Error> {
        let buckets = value
            .buckets
            .into_iter()
            .map(HistogramBucket::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        Histogram::try_from_buckets(value.accuracy, buckets, value.avg_spacing)
    }
}

#[derive(Serialize, Deserialize)]
struct LegacyHistogramBucket {
    lower_bound: Datum,
    upper_bound: Datum,
    num_values: f64,
    num_distinct: f64,
}

impl From<HistogramBucket> for LegacyHistogramBucket {
    fn from(bucket: HistogramBucket) -> Self {
        Self {
            lower_bound: bucket.lower_bound(),
            upper_bound: bucket.upper_bound(),
            num_values: bucket.num_values(),
            num_distinct: bucket.num_distinct(),
        }
    }
}

impl TryFrom<LegacyHistogramBucket> for HistogramBucket {
    type Error = &'static str;

    fn try_from(value: LegacyHistogramBucket) -> Result<Self, Self::Error> {
        HistogramBucket::try_from_bounds(
            value.lower_bound,
            value.upper_bound,
            value.num_values,
            value.num_distinct,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use databend_common_statistics::F64;
    use databend_common_statistics::Histogram;
    use databend_common_statistics::TypedHistogram;
    use databend_common_statistics::TypedHistogramBucket;
    use serde::Deserialize;
    use serde::Serialize;

    #[derive(Serialize, Deserialize)]
    struct TestStatistics {
        #[serde(with = "super")]
        histograms: HashMap<u32, Histogram>,
    }

    #[test]
    fn deserialize_discards_failed_histogram_entries() {
        let json = r#"{
            "histograms": {
                "1": {
                    "accuracy": true,
                    "buckets": [
                        {
                            "lower_bound": {"Int": 1},
                            "upper_bound": {"Int": 10},
                            "num_values": 10.0,
                            "num_distinct": 10.0
                        }
                    ]
                },
                "2": {
                    "accuracy": true,
                    "buckets": [
                        {
                            "lower_bound": {"Int": 1},
                            "upper_bound": {"Bytes": [120]},
                            "num_values": 10.0,
                            "num_distinct": 10.0
                        }
                    ]
                },
                "3": {
                    "accuracy": true,
                    "buckets": []
                }
            }
        }"#;

        let stats: TestStatistics = serde_json::from_str(json).unwrap();
        assert_eq!(stats.histograms.len(), 1);
        assert!(matches!(stats.histograms.get(&1), Some(Histogram::Int(_))));
        assert!(!stats.histograms.contains_key(&2));
        assert!(!stats.histograms.contains_key(&3));
    }

    #[test]
    fn serialize_uses_legacy_snapshot_statistics_shape() {
        let stats = TestStatistics {
            histograms: HashMap::from([(
                1,
                Histogram::Float(TypedHistogram {
                    accuracy: false,
                    buckets: vec![TypedHistogramBucket::new(
                        F64::from(1.0),
                        F64::from(2.0),
                        3.0,
                        2.0,
                    )],
                    avg_spacing: Some(0.5),
                }),
            )]),
        };

        let value = serde_json::to_value(stats).unwrap();
        assert_eq!(value["histograms"]["1"]["accuracy"], false);
        assert_eq!(value["histograms"]["1"]["avg_spacing"], 0.5);
        assert!(value["histograms"]["1"]["buckets"].is_array());
        assert!(value["histograms"]["1"].get("Float").is_none());
    }
}
