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

use std::cmp::Ordering;
use std::collections::HashMap;

use common_expression::converts::from_scalar;
use common_expression::ColumnId;
use common_expression::Scalar;
use common_expression::TableDataType;
use common_expression::TableField;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum MinMax {
    // min eq max
    Point(Scalar),
    // inclusive on both sides, min < max
    Range((Scalar, Scalar)),
}

impl MinMax {
    pub fn new(v1: Scalar, v2: Scalar) -> Self {
        match v1.cmp(&v2) {
            Ordering::Equal => MinMax::Point(v1),
            Ordering::Less => MinMax::Range((v1, v2)),
            Ordering::Greater => MinMax::Range((v2, v1)),
        }
    }

    pub fn new_with_scalars(v1: Vec<Scalar>, v2: Vec<Scalar>) -> Self {
        assert_eq!(v1.len(), v2.len());

        let (v1, v2) = if v1.len() == 1 {
            (v1[0].clone(), v2[0].clone())
        } else {
            (Scalar::Tuple(v1), Scalar::Tuple(v2))
        };

        MinMax::new(v1, v2)
    }

    pub fn min(&self) -> &Scalar {
        match self {
            MinMax::Point(v) => v,
            MinMax::Range((v, _)) => v,
        }
    }

    pub fn max(&self) -> &Scalar {
        match self {
            MinMax::Point(v) => v,
            MinMax::Range((v, _)) => v,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ColumnStatistics {
    pub minmax: MinMax,

    pub null_count: u64,
    pub in_memory_size: u64,
    pub distinct_of_values: Option<u64>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ClusterStatistics {
    pub cluster_key_id: u32,
    pub minmax: MinMax,
    pub level: i32,

    // currently it's only used in native engine
    pub pages: Option<Vec<Scalar>>,
}

impl ColumnStatistics {
    pub fn new(
        min: Scalar,
        max: Scalar,
        null_count: u64,
        in_memory_size: u64,
        distinct_of_values: Option<u64>,
    ) -> Self {
        Self {
            minmax: MinMax::new(min, max),
            null_count,
            in_memory_size,
            distinct_of_values,
        }
    }

    pub fn min(&self) -> &Scalar {
        self.minmax.min()
    }

    pub fn max(&self) -> &Scalar {
        self.minmax.max()
    }

    pub fn from_v2(v2: &crate::meta::v2::statistics::ColumnStatistics) -> Self {
        Self {
            minmax: MinMax::new(v2.min.clone(), v2.max.clone()),
            null_count: v2.null_count,
            in_memory_size: v2.in_memory_size,
            distinct_of_values: v2.distinct_of_values,
        }
    }
}

impl ClusterStatistics {
    pub fn new(
        cluster_key_id: u32,
        min: Vec<Scalar>,
        max: Vec<Scalar>,
        level: i32,
        pages: Option<Vec<Scalar>>,
    ) -> Self {
        Self {
            cluster_key_id,
            minmax: MinMax::new_with_scalars(min, max),
            level,
            pages,
        }
    }

    pub fn min(&self) -> Scalar {
        self.minmax.min().clone()
    }

    pub fn max(&self) -> Scalar {
        self.minmax.max().clone()
    }

    pub fn is_const(&self) -> bool {
        matches!(self.minmax, MinMax::Point(_))
    }

    pub fn from_v2(v2: crate::meta::v2::statistics::ClusterStatistics) -> Self {
        Self {
            cluster_key_id: v2.cluster_key_id,
            minmax: MinMax::new_with_scalars(v2.min, v2.max),
            level: v2.level,
            pages: v2.pages,
        }
    }
}
