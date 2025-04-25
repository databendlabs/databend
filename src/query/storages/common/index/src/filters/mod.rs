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

//! Probabilistic filters

mod filter;
mod xor8;

pub use filter::Filter;
pub use filter::FilterBuilder;
pub use xor8::BlockBloomFilterIndexVersion;
pub use xor8::BlockFilter;
pub use xor8::BloomBuilder;
pub use xor8::BloomBuildingError;
pub use xor8::BloomCodecError;
pub use xor8::BloomFilter;
pub use xor8::FilterImpl;
pub use xor8::FilterImplBuilder;
pub use xor8::V2BloomBlock;
pub use xor8::Xor8Builder;
pub use xor8::Xor8BuildingError;
pub use xor8::Xor8CodecError;
pub use xor8::Xor8Filter;
