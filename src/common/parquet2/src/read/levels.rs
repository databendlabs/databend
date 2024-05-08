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

/// Returns the number of bits needed to store the given maximum definition or repetition level.
#[inline]
pub fn get_bit_width(max_level: i16) -> u32 {
    16 - max_level.leading_zeros()
}

#[cfg(test)]
mod tests {
    use super::get_bit_width;

    #[test]
    fn test_get_bit_width() {
        assert_eq!(0, get_bit_width(0));
        assert_eq!(1, get_bit_width(1));
        assert_eq!(2, get_bit_width(2));
        assert_eq!(2, get_bit_width(3));
        assert_eq!(3, get_bit_width(4));
        assert_eq!(3, get_bit_width(5));
        assert_eq!(3, get_bit_width(6));
        assert_eq!(3, get_bit_width(7));
        assert_eq!(4, get_bit_width(8));
        assert_eq!(4, get_bit_width(15));

        assert_eq!(8, get_bit_width(255));
        assert_eq!(9, get_bit_width(256));
    }
}
