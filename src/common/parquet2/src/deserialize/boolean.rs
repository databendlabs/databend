// Copyright [2021] [Jorge C Leitao]
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

use super::utils;
use crate::encoding::hybrid_rle::BitmapIter;
use crate::error::Error;
use crate::page::split_buffer;
use crate::page::DataPage;
use crate::parquet_bridge::Encoding;
use crate::parquet_bridge::Repetition;

// The state of a `DataPage` of `Boolean` parquet boolean type
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum BooleanPageState<'a> {
    Optional(utils::DefLevelsDecoder<'a>, BitmapIter<'a>),
    Required(&'a [u8], usize),
}

impl<'a> BooleanPageState<'a> {
    pub fn try_new(page: &'a DataPage) -> Result<Self, Error> {
        let is_optional =
            page.descriptor.primitive_type.field_info.repetition == Repetition::Optional;

        match (page.encoding(), is_optional) {
            (Encoding::Plain, true) => {
                let validity = utils::DefLevelsDecoder::try_new(page)?;

                let (_, _, values) = split_buffer(page)?;
                let values = BitmapIter::new(values, 0, values.len() * 8);

                Ok(Self::Optional(validity, values))
            }
            (Encoding::Plain, false) => {
                let (_, _, values) = split_buffer(page)?;
                Ok(Self::Required(values, page.num_values()))
            }
            _ => Err(Error::InvalidParameter(format!(
                "Viewing page for encoding {:?} for boolean type not supported",
                page.encoding(),
            ))),
        }
    }
}
