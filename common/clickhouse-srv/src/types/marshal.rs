// Copyright 2020 Datafuse Labs.
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
// limitations under the License

pub trait Marshal {
    fn marshal(&self, scratch: &mut [u8]);
}

impl Marshal for u8 {
    fn marshal(&self, scratch: &mut [u8]) {
        scratch[0] = *self;
    }
}

impl Marshal for u16 {
    fn marshal(&self, scratch: &mut [u8]) {
        scratch[0] = *self as u8;
        scratch[1] = (self >> 8) as u8;
    }
}

impl Marshal for u32 {
    fn marshal(&self, scratch: &mut [u8]) {
        scratch[0] = *self as u8;
        scratch[1] = (self >> 8) as u8;
        scratch[2] = (self >> 16) as u8;
        scratch[3] = (self >> 24) as u8;
    }
}

impl Marshal for u64 {
    fn marshal(&self, scratch: &mut [u8]) {
        scratch[0] = *self as u8;
        scratch[1] = (self >> 8) as u8;
        scratch[2] = (self >> 16) as u8;
        scratch[3] = (self >> 24) as u8;

        scratch[4] = (self >> 32) as u8;
        scratch[5] = (self >> 40) as u8;
        scratch[6] = (self >> 48) as u8;
        scratch[7] = (self >> 56) as u8;
    }
}

impl Marshal for i8 {
    fn marshal(&self, scratch: &mut [u8]) {
        scratch[0] = *self as u8;
    }
}

impl Marshal for i16 {
    fn marshal(&self, scratch: &mut [u8]) {
        scratch[0] = *self as u8;
        scratch[1] = (self >> 8) as u8;
    }
}

impl Marshal for i32 {
    fn marshal(&self, scratch: &mut [u8]) {
        scratch[0] = *self as u8;
        scratch[1] = (self >> 8) as u8;
        scratch[2] = (self >> 16) as u8;
        scratch[3] = (self >> 24) as u8;
    }
}

impl Marshal for i64 {
    fn marshal(&self, scratch: &mut [u8]) {
        scratch[0] = *self as u8;
        scratch[1] = (self >> 8) as u8;
        scratch[2] = (self >> 16) as u8;
        scratch[3] = (self >> 24) as u8;

        scratch[4] = (self >> 32) as u8;
        scratch[5] = (self >> 40) as u8;
        scratch[6] = (self >> 48) as u8;
        scratch[7] = (self >> 56) as u8;
    }
}

impl Marshal for f32 {
    fn marshal(&self, scratch: &mut [u8]) {
        let bits = self.to_bits();
        scratch[0] = bits as u8;
        scratch[1] = (bits >> 8) as u8;
        scratch[2] = (bits >> 16) as u8;
        scratch[3] = (bits >> 24) as u8;
    }
}

impl Marshal for f64 {
    fn marshal(&self, scratch: &mut [u8]) {
        let bits = self.to_bits();
        scratch[0] = bits as u8;
        scratch[1] = (bits >> 8) as u8;
        scratch[2] = (bits >> 16) as u8;
        scratch[3] = (bits >> 24) as u8;
        scratch[4] = (bits >> 32) as u8;
        scratch[5] = (bits >> 40) as u8;
        scratch[6] = (bits >> 48) as u8;
        scratch[7] = (bits >> 56) as u8;
    }
}

impl Marshal for bool {
    fn marshal(&self, scratch: &mut [u8]) {
        scratch[0] = *self as u8;
    }
}

#[cfg(test)]
mod test {
    use std::fmt;

    use rand::distributions::Distribution;
    use rand::distributions::Standard;
    use rand::random;

    use crate::types::Marshal;
    use crate::types::StatBuffer;
    use crate::types::Unmarshal;

    fn test_some<T>()
    where
        T: Copy + fmt::Debug + StatBuffer + Marshal + Unmarshal<T> + PartialEq,
        Standard: Distribution<T>,
    {
        for _ in 0..100 {
            let mut buffer = T::buffer();
            let v = random::<T>();

            v.marshal(buffer.as_mut());
            let u = T::unmarshal(buffer.as_ref());

            assert_eq!(v, u);
        }
    }

    #[test]
    fn test_u8() {
        test_some::<u8>()
    }

    #[test]
    fn test_u16() {
        test_some::<u16>()
    }

    #[test]
    fn test_u32() {
        test_some::<u32>()
    }

    #[test]
    fn test_u64() {
        test_some::<u64>()
    }

    #[test]
    fn test_i8() {
        test_some::<i8>()
    }

    #[test]
    fn test_i16() {
        test_some::<i16>()
    }

    #[test]
    fn test_i32() {
        test_some::<i32>()
    }

    #[test]
    fn test_i64() {
        test_some::<i64>()
    }

    #[test]
    fn test_f32() {
        test_some::<f32>()
    }

    #[test]
    fn test_f64() {
        test_some::<f64>()
    }

    #[test]
    fn test_bool() {
        test_some::<bool>()
    }
}
