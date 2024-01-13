// Copyright 2020-2022 Jorge C. Leitão
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

#[cfg(feature = "compute_aggregate")]
mod aggregate;
#[cfg(feature = "compute_cast")]
mod cast;
#[cfg(feature = "compute_concatenate")]
mod concatenate;
#[cfg(feature = "compute_merge_sort")]
mod merge_sort;
#[cfg(feature = "compute_sort")]
mod sort;
#[cfg(feature = "compute_take")]
mod take;
