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

use databend_common_expression::Domain;
use databend_common_expression::types::DataType;
use databend_common_expression::types::boolean::BooleanDomain;
use databend_common_expression::types::nullable::NullableDomain;
use geo::Rect;

use crate::IndexFile;
use crate::IndexMeta;

pub type SpatialIndexMeta = IndexMeta;
pub type SpatialIndexFile = IndexFile;

pub fn rects_intersect(block_rect: &Rect<f64>, query_rect: &Option<Rect<f64>>) -> bool {
    if let Some(query_rect) = query_rect {
        block_rect.min().x <= query_rect.max().x
            && block_rect.max().x >= query_rect.min().x
            && block_rect.min().y <= query_rect.max().y
            && block_rect.max().y >= query_rect.min().y
    } else {
        false
    }
}

pub fn rect_contains(block_rect: &Rect<f64>, query_rect: &Option<Rect<f64>>) -> bool {
    if let Some(query_rect) = query_rect {
        block_rect.min().x <= query_rect.min().x
            && block_rect.min().y <= query_rect.min().y
            && block_rect.max().x >= query_rect.max().x
            && block_rect.max().y >= query_rect.max().y
    } else {
        false
    }
}

pub fn spatial_false_domain(return_type: &DataType, has_null: bool) -> Domain {
    let bool_domain = Domain::Boolean(BooleanDomain {
        has_false: true,
        has_true: false,
    });
    if return_type.is_nullable() {
        Domain::Nullable(NullableDomain {
            has_null,
            value: Some(Box::new(bool_domain)),
        })
    } else {
        bool_domain
    }
}
