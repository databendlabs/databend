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

pub use streaming_iterator::StreamingIterator;

/// A [`StreamingIterator`] with an internal buffer of [`Vec<u8>`] used to efficiently
/// present items of type `T` as `&[u8]`.
/// It is generic over the type `T` and the transformation `F: T -> &[u8]`.
pub struct BufStreamingIterator<I, F, T>
where
    I: Iterator<Item = T>,
    F: FnMut(T, &mut Vec<u8>),
{
    iterator: I,
    f: F,
    buffer: Vec<u8>,
    is_valid: bool,
}

impl<I, F, T> BufStreamingIterator<I, F, T>
where
    I: Iterator<Item = T>,
    F: FnMut(T, &mut Vec<u8>),
{
    #[inline]
    pub fn new(iterator: I, f: F, buffer: Vec<u8>) -> Self {
        Self {
            iterator,
            f,
            buffer,
            is_valid: false,
        }
    }
}

impl<I, F, T> StreamingIterator for BufStreamingIterator<I, F, T>
where
    I: Iterator<Item = T>,
    F: FnMut(T, &mut Vec<u8>),
{
    type Item = [u8];

    #[inline]
    fn advance(&mut self) {
        let a = self.iterator.next();
        if let Some(a) = a {
            self.is_valid = true;
            self.buffer.clear();
            (self.f)(a, &mut self.buffer);
        } else {
            self.is_valid = false;
        }
    }

    #[inline]
    fn get(&self) -> Option<&Self::Item> {
        if self.is_valid {
            Some(&self.buffer)
        } else {
            None
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iterator.size_hint()
    }
}
