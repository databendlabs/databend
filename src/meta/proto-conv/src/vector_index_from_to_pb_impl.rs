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

//! This mod is the key point about compatibility.
//! Everytime update anything in this file, update the `VER` and let the tests pass.

use common_vector::index::VectorIndex;

use crate::FromToProto;
use crate::Incompatible;

impl FromToProto for VectorIndex {
    type PB = String;
    fn get_pb_ver(_p: &Self::PB) -> u64 {
        0
    }

    fn to_pb(&self) -> Result<String, Incompatible> {
        let pb = match self {
            VectorIndex::IvfFlat(v) => format!("IvfFlat_{}_{}", v.nlist, v.nprobe),
        };
        Ok(pb)
    }

    fn from_pb(p: String) -> Result<Self, Incompatible> {
        let v: Vec<_> = p.split('_').collect();
        if v.is_empty() {
            return Err(Incompatible {
                reason: format!("Invalid index: {}", p),
            });
        }
        let index_type = v[0];
        match index_type {
            "IvfFlat" => {
                if v.len() != 3 {
                    return Err(Incompatible {
                        reason: format!("Invalid index: {}", p),
                    });
                }
                let nlists = v[1].parse::<usize>().map_err(|e| Incompatible {
                    reason: format!("Invalid index: {}, {}", p, e),
                })?;
                let nprobe = v[2].parse::<usize>().map_err(|e| Incompatible {
                    reason: format!("Invalid index: {}, {}", p, e),
                })?;
                Ok(VectorIndex::IvfFlat(common_vector::index::IvfFlatIndex {
                    nlist: nlists,
                    nprobe,
                }))
            }
            _ => Err(Incompatible {
                reason: format!("Unknown index type: {}", index_type),
            }),
        }
    }
}
