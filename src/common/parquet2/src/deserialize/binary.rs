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
use crate::encoding::hybrid_rle;
use crate::encoding::plain_byte_array::BinaryIter;
use crate::error::Error;
use crate::page::split_buffer;
use crate::page::DataPage;
use crate::parquet_bridge::Encoding;
use crate::parquet_bridge::Repetition;

#[derive(Debug)]
pub struct Dictionary<'a, P> {
    pub indexes: hybrid_rle::HybridRleDecoder<'a>,
    pub dict: P,
}

impl<'a, P> Dictionary<'a, P> {
    pub fn try_new(page: &'a DataPage, dict: P) -> Result<Self, Error> {
        let indexes = utils::dict_indices_decoder(page)?;

        Ok(Self { indexes, dict })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.indexes.size_hint().0
    }
}

#[allow(clippy::large_enum_variant)]
pub enum BinaryPageState<'a, P> {
    Optional(utils::DefLevelsDecoder<'a>, BinaryIter<'a>),
    Required(BinaryIter<'a>),
    RequiredDictionary(Dictionary<'a, P>),
    OptionalDictionary(utils::DefLevelsDecoder<'a>, Dictionary<'a, P>),
}

impl<'a, P> BinaryPageState<'a, P> {
    pub fn try_new(page: &'a DataPage, dict: Option<P>) -> Result<Self, Error> {
        let is_optional =
            page.descriptor.primitive_type.field_info.repetition == Repetition::Optional;

        match (page.encoding(), dict, is_optional) {
            (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), false) => {
                Dictionary::try_new(page, dict).map(Self::RequiredDictionary)
            }
            (Encoding::PlainDictionary | Encoding::RleDictionary, Some(dict), true) => {
                Ok(Self::OptionalDictionary(
                    utils::DefLevelsDecoder::try_new(page)?,
                    Dictionary::try_new(page, dict)?,
                ))
            }
            (Encoding::Plain, _, true) => {
                let (_, _, values) = split_buffer(page)?;

                let validity = utils::DefLevelsDecoder::try_new(page)?;
                let values = BinaryIter::new(values, None);

                Ok(Self::Optional(validity, values))
            }
            (Encoding::Plain, _, false) => {
                let (_, _, values) = split_buffer(page)?;
                let values = BinaryIter::new(values, Some(page.num_values()));

                Ok(Self::Required(values))
            }
            _ => Err(Error::FeatureNotSupported(format!(
                "Viewing page for encoding {:?} for binary type",
                page.encoding(),
            ))),
        }
    }
}
