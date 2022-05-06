//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::marker::PhantomData;

use common_datablocks::DataBlock;
use common_exception::ErrorCode;

use crate::storages::fuse::meta::common::Versioned;
use crate::storages::fuse::meta::v0;
use crate::storages::fuse::meta::v1;

// Here versions of meta are tagged with numeric values
//
// The trait Versioned itself can not prevent us from
// giving multiple version numbers to a specific type, such as
//
// impl Versioned<0> for v0::SegmentInfo {}
// impl Versioned<1> for v0::SegmentInfo {}
//
// Fortunately, since v0::SegmentInfo::VESION is used in
// several places, compiler will report compile error if it
// can not deduce a unique value the constant expression.

impl Versioned<0> for v0::SegmentInfo {}
impl Versioned<1> for v1::SegmentInfo {}

pub enum SegmentInfoVersion {
    V0(PhantomData<v0::SegmentInfo>),
    V1(PhantomData<v1::SegmentInfo>),
}

impl Versioned<0> for v0::TableSnapshot {}
impl Versioned<1> for v1::TableSnapshot {}

pub enum SnapshotVersion {
    V0(PhantomData<v0::TableSnapshot>),
    V1(PhantomData<v1::TableSnapshot>),
}

impl SnapshotVersion {
    pub fn version(&self) -> u64 {
        match self {
            SnapshotVersion::V0(a) => Self::ver(a),
            SnapshotVersion::V1(a) => Self::ver(a),
        }
    }

    fn ver<const V: u64, T: Versioned<V>>(_v: &PhantomData<T>) -> u64 {
        V
    }
}

impl Versioned<0> for DataBlock {}

mod converters {

    use super::*;

    impl TryFrom<u64> for SegmentInfoVersion {
        type Error = ErrorCode;
        fn try_from(value: u64) -> std::result::Result<Self, Self::Error> {
            match value {
                0 => Ok(SegmentInfoVersion::V0(
                    // `ver_eq<_, 0>` is merely a static check to make sure that
                    // the type `v0::SegmentInfoVersion` do have a numeric version number of 0
                    ver_eq::<_, 0>(PhantomData),
                )),
                1 => Ok(SegmentInfoVersion::V1(ver_eq::<_, 1>(PhantomData))),
                _ => Err(ErrorCode::LogicalError(format!(
                    "unknown segment version {value}, versions supported: 0, 1"
                ))),
            }
        }
    }

    impl TryFrom<u64> for SnapshotVersion {
        type Error = ErrorCode;
        fn try_from(value: u64) -> std::result::Result<Self, Self::Error> {
            match value {
                0 => Ok(SnapshotVersion::V0(ver_eq::<_, 0>(PhantomData))),
                1 => Ok(SnapshotVersion::V1(ver_eq::<_, 1>(PhantomData))),
                _ => Err(ErrorCode::LogicalError(format!(
                    "unknown snapshot segment version {value}, versions supported: 0, 1"
                ))),
            }
        }
    }

    /// Statically check that if T implements Versoined<U> where U equals V
    #[inline]
    fn ver_eq<T, const V: u64>(t: PhantomData<T>) -> PhantomData<T>
    where T: Versioned<V> {
        t
    }
}
