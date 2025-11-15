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

use std::sync::Arc;

use ahash::AHashMap;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::Value;
use parking_lot::RwLock;

use super::meta::UniqueKeyDigest;

#[derive(Clone, Debug)]
pub enum AutoIncValueSource {
    Key(usize),
    Remain(usize),
}

#[derive(Clone, Debug)]
pub struct AutoIncColumn {
    pub field_index: usize,
    pub source: AutoIncValueSource,
}

#[derive(Clone, Debug)]
pub struct AutoIncrementReplacer {
    columns: Vec<AutoIncColumn>,
    values: Arc<RwLock<AHashMap<UniqueKeyDigest, Vec<Scalar>>>>,
}

impl AutoIncrementReplacer {
    pub fn try_new(columns: Vec<AutoIncColumn>) -> Option<Arc<Self>> {
        if columns.is_empty() {
            return None;
        }
        Some(Arc::new(Self {
            columns,
            values: Arc::new(RwLock::new(AHashMap::default())),
        }))
    }

    pub fn columns(&self) -> &[AutoIncColumn] {
        &self.columns
    }

    pub fn record(&self, hash: UniqueKeyDigest, row_values: Vec<Scalar>) -> Result<()> {
        if row_values.len() != self.columns.len() {
            return Err(databend_common_exception::ErrorCode::Internal(format!(
                "auto increment replacement expects {} values, got {}",
                self.columns.len(),
                row_values.len()
            )));
        }
        self.values.write().insert(hash, row_values);
        Ok(())
    }

    pub fn take(&self, hash: &UniqueKeyDigest) -> Option<Vec<Scalar>> {
        self.values.write().remove(hash)
    }
}

pub fn scalar_ref_to_owned(scalar_ref: ScalarRef) -> Scalar {
    match scalar_ref {
        ScalarRef::Null => Scalar::Null,
        ScalarRef::EmptyArray => Scalar::EmptyArray,
        ScalarRef::EmptyMap => Scalar::EmptyMap,
        ScalarRef::Number(n) => Scalar::Number(n),
        ScalarRef::Decimal(d) => Scalar::Decimal(d),
        ScalarRef::Boolean(b) => Scalar::Boolean(b),
        ScalarRef::Binary(v) => Scalar::Binary(v.to_vec()),
        ScalarRef::String(v) => Scalar::String(v.to_string()),
        ScalarRef::Timestamp(t) => Scalar::Timestamp(t),
        ScalarRef::TimestampTz(t) => Scalar::TimestampTz(t),
        ScalarRef::Date(d) => Scalar::Date(d),
        ScalarRef::Interval(val) => Scalar::Interval(val),
        ScalarRef::Array(col) => Scalar::Array(col),
        ScalarRef::Map(col) => Scalar::Map(col),
        ScalarRef::Bitmap(v) => Scalar::Bitmap(v.to_vec()),
        ScalarRef::Tuple(items) => Scalar::Tuple(items.into_iter().map(scalar_ref_to_owned).collect()),
        ScalarRef::Variant(v) => Scalar::Variant(v.to_vec()),
        ScalarRef::Geometry(v) => Scalar::Geometry(v.to_vec()),
        ScalarRef::Geography(v) => Scalar::Geography(v.into()),
        ScalarRef::Vector(v) => Scalar::Vector(v.to_owned()),
        ScalarRef::Opaque(v) => Scalar::Opaque(v.to_owned()),
    }
}

pub fn value_row_scalar(value: &Value<databend_common_expression::types::AnyType>, row: usize) -> Result<Scalar> {
    let scalar_ref = value.row_scalar(row)?;
    Ok(scalar_ref_to_owned(scalar_ref))
}
