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

//! Verus entry point only: this file is not part of the Cargo crate, and no
//! Cargo build, test or lint compiles it. Verify with the pinned Verus release
//! (0.2026.10.18.7d82994, Rust 1.99.1):
//!
//! ```text
//! verus --no-cheating src/query/functions/src/scalars/decimal/src/arithmetic/verify.rs
//! ```
//!
//! It loads the production `decision.rs` with a local DecimalSize input
//! interface. The local type is a temporary workaround: Verus lags behind the
//! nightly toolchain this workspace uses, so it cannot compile the expression
//! crate, which relies on newer Rust APIs. Replace it with the real type once
//! Verus supports a recent enough Rust.
//!
//! Accessor correspondence to the real type is a trusted boundary, not a claim
//! proved by Verus. No values cross between the types, so there is no cast or
//! ABI/layout assumption. These fields describe only the local input interface.

use vstd::prelude::*;

// Verus supplies verus_keep_ghost to select this input interface. Normal Cargo
// builds use the real expression type. Both modes load identical decisions.
mod decision;

#[verus_verify]
#[derive(Clone, Copy, Debug)]
struct DecimalSize {
    precision: u8,
    scale: u8,
}

#[verus_verify]
impl DecimalSize {
    #[verus_spec(out =>
        ensures
            out is Ok <==> 1 <= precision <= 76 && scale <= precision,
            out matches Ok(size) ==> size.precision == precision && size.scale == scale,
    )]
    fn new(precision: u8, scale: u8) -> Result<DecimalSize, ()> {
        if 1 <= precision && precision <= 76 && scale <= precision {
            Ok(DecimalSize { precision, scale })
        } else {
            Err(())
        }
    }

    #[verus_spec(out => ensures out == self.precision)]
    fn precision(&self) -> u8 {
        self.precision
    }

    #[verus_spec(out => ensures out == self.scale)]
    fn scale(&self) -> u8 {
        self.scale
    }

    #[verus_spec(out =>
        requires self.scale <= self.precision,
        ensures out == self.precision as int - self.scale as int,
    )]
    fn leading_digits(&self) -> u8 {
        self.precision - self.scale
    }
}

fn main() {}
