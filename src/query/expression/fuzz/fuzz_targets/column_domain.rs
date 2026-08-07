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

#![no_main]

use databend_expression_domain_fuzz::ColumnCase;
use databend_expression_domain_fuzz::run_column_case;
use libfuzzer_sys::fuzz_target;

// This target validates the fundamental `Column::domain` invariant: every value physically present
// in a generated column must be contained by the domain returned for that column.

// The typed `Arbitrary` generator covers empty and NULL columns, all number and decimal widths,
// Boolean, String, temporal and interval columns, Binary, and recursive Nullable, Array, Map, and
// Tuple shapes. Array and Map offsets may start above zero so sliced-column behavior is exercised as
// well.

// For every row, the oracle recursively checks the actual scalar value against the column domain.
// Nullable checks NULL membership, Array and Map check their real elements, and Tuple checks each
// field. Primitive values use singleton-domain containment. Undefined domains accept unsupported
// scalar types by definition.
//
// cargo fuzz run --dev --sanitizer none --strip-dead-code column_domain -- -runs=10000 -max_len=4096
fuzz_target!(|case: ColumnCase| run_column_case(case));
