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

// macro implementing `sliced` and `sliced_unchecked`
#[macro_export]
macro_rules! impl_sliced {
    () => {
        /// Returns this array sliced.
        /// # Implementation
        /// This function is `O(1)`.
        /// # Panics
        /// iff `offset + length > self.len()`.
        #[inline]
        #[must_use]
        pub fn sliced(self, offset: usize, length: usize) -> Self {
            assert!(
                offset + length <= self.len(),
                "the offset of the new Buffer cannot exceed the existing length"
            );
            unsafe { self.sliced_unchecked(offset, length) }
        }

        /// Returns this array sliced.
        /// # Implementation
        /// This function is `O(1)`.
        /// # Safety
        /// The caller must ensure that `offset + length <= self.len()`.
        #[inline]
        #[must_use]
        pub unsafe fn sliced_unchecked(mut self, offset: usize, length: usize) -> Self {
            unsafe {
                self.slice_unchecked(offset, length);
                self
            }
        }
    };
}

#[macro_export]
macro_rules! with_number_type {
    ( | $t:tt | $($tail:tt)* ) => {
        match_template::match_template! {
            $t = [UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64],
            $($tail)*
        }
    }
}
