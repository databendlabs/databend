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

use arrow_buffer::i256;
use databend_common_expression::types::decimal::DecimalScalar;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::Scalar;
use ethnum::I256;
use parquet::data_type::AsBytes;
use parquet::data_type::FixedLenByteArray;

pub fn decode_decimal128_from_bytes(arr: &FixedLenByteArray, size: DecimalSize) -> Scalar {
    let v = i128::from_be_bytes(sign_extend_be(arr.as_bytes()));
    Scalar::Decimal(DecimalScalar::Decimal128(v, size))
}

pub fn decode_decimal256_from_bytes(arr: &FixedLenByteArray, size: DecimalSize) -> Scalar {
    let v = i256::from_be_bytes(sign_extend_be(arr.as_bytes()));
    let (lo, hi) = v.to_parts();
    let v = I256::from_words(hi, lo as i128);
    Scalar::Decimal(DecimalScalar::Decimal256(v, size))
}

// from arrow-rs
fn sign_extend_be<const N: usize>(b: &[u8]) -> [u8; N] {
    assert!(b.len() <= N, "Array too large, expected less than {N}");
    let is_negative = (b[0] & 128u8) == 128u8;
    let mut result = if is_negative { [255u8; N] } else { [0u8; N] };
    for (d, s) in result.iter_mut().skip(N - b.len()).zip(b) {
        *d = *s;
    }
    result
}
