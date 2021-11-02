// Copyright 2021 Datafuse Labs.
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

use std::convert::TryInto;

use serde::Deserialize;
use serde::Serialize;

/// The meta data of a record in kv
#[derive(Serialize, Deserialize, Debug, Default, Clone, Eq, PartialEq)]
pub struct KVMeta {
    /// expiration time in second since 1970
    pub expire_at: Option<u64>,
}

/// Some value bound with a seq number
#[derive(Serialize, Deserialize, Debug, Default, Clone, Eq, PartialEq)]
pub struct SeqV<T = Vec<u8>> {
    pub seq: u64,
    pub meta: Option<KVMeta>,
    pub data: T,
}

pub trait IntoSeqV<T> {
    type Error;
    fn into_seqv(self) -> Result<SeqV<T>, Self::Error>;
}

impl<T, V> IntoSeqV<T> for SeqV<V>
where V: TryInto<T>
{
    type Error = <V as TryInto<T>>::Error;

    fn into_seqv(self) -> Result<SeqV<T>, Self::Error> {
        Ok(SeqV {
            seq: self.seq,
            meta: self.meta,
            data: self.data.try_into()?,
        })
    }
}

impl<T> SeqV<T> {
    pub fn new(seq: u64, data: T) -> Self {
        Self {
            seq,
            meta: None,
            data,
        }
    }

    pub fn with_meta(seq: u64, meta: Option<KVMeta>, data: T) -> Self {
        Self { seq, meta, data }
    }

    pub fn get_expire_at(&self) -> u64 {
        match self.meta {
            None => u64::MAX,
            Some(ref m) => match m.expire_at {
                None => u64::MAX,
                Some(exp_at) => exp_at,
            },
        }
    }

    pub fn set_seq(mut self, seq: u64) -> SeqV<T> {
        self.seq = seq;
        self
    }

    pub fn set_meta(mut self, m: Option<KVMeta>) -> SeqV<T> {
        self.meta = m;
        self
    }

    pub fn set_value(mut self, v: T) -> SeqV<T> {
        self.data = v;
        self
    }
}
