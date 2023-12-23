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

use std::cell::UnsafeCell;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use databend_common_arrow::parquet::error::Error;
use databend_common_arrow::parquet::page::CompressedPage;
use databend_common_arrow::parquet::page::Page;
use databend_common_arrow::parquet::read::decompress;
use databend_common_arrow::parquet::FallibleStreamingIterator;
use databend_common_exception::Result;
use streaming_decompression::Compressed;
use streaming_decompression::Decompressed;

// Note: cannot be accessed between multiple threads at the same time.
pub struct UncompressedBuffer {
    used: AtomicUsize,
    buffer: UnsafeCell<Vec<u8>>,
}

unsafe impl Send for UncompressedBuffer {}

unsafe impl Sync for UncompressedBuffer {}

impl UncompressedBuffer {
    pub fn new(capacity: usize) -> Arc<UncompressedBuffer> {
        Arc::new(UncompressedBuffer {
            used: AtomicUsize::new(0),
            buffer: UnsafeCell::new(Vec::with_capacity(capacity)),
        })
    }

    pub fn clear(self: &Arc<Self>) {
        let guard = self.borrow_mut();

        if !guard.is_unique_borrow_mut() {
            panic!(
                "UncompressedBuffer cannot be accessed between multiple threads at the same time."
            );
        }

        drop(std::mem::take(self.buffer_mut()));
    }

    #[allow(clippy::mut_from_ref)]
    pub(in crate::io::read::block::decompressor) fn buffer_mut(&self) -> &mut Vec<u8> {
        unsafe { &mut *self.buffer.get() }
    }

    pub(in crate::io::read::block::decompressor) fn borrow_mut(self: &Arc<Self>) -> UsedGuard {
        UsedGuard::create(self)
    }
}

pub struct BuffedBasicDecompressor<I: Iterator<Item = Result<CompressedPage, Error>>> {
    iter: I,
    current: Option<Page>,
    was_decompressed: bool,
    uncompressed_buffer: Arc<UncompressedBuffer>,
}

impl<I: Iterator<Item = Result<CompressedPage, Error>>> BuffedBasicDecompressor<I> {
    pub fn new(iter: I, uncompressed_buffer: Arc<UncompressedBuffer>) -> Self {
        Self {
            iter,
            current: None,
            uncompressed_buffer,
            was_decompressed: false,
        }
    }
}

impl<I> FallibleStreamingIterator for BuffedBasicDecompressor<I>
where I: Iterator<Item = Result<CompressedPage, Error>>
{
    type Item = Page;
    type Error = Error;

    #[inline]
    fn advance(&mut self) -> Result<(), Error> {
        if let Some(page) = self.current.as_mut() {
            if self.was_decompressed {
                let guard = self.uncompressed_buffer.borrow_mut();

                if !guard.is_unique_borrow_mut() {
                    return Err(Error::FeatureNotSupported(String::from(
                        "UncompressedBuffer cannot be accessed between multiple threads at the same time.",
                    )));
                }

                {
                    let borrow_buffer = self.uncompressed_buffer.buffer_mut();

                    if borrow_buffer.capacity() < page.buffer_mut().capacity() {
                        *borrow_buffer = std::mem::take(page.buffer_mut());
                    }
                }
            }
        }

        self.current = match self.iter.next() {
            None => None,
            Some(page) => {
                let guard = self.uncompressed_buffer.borrow_mut();

                if !guard.is_unique_borrow_mut() {
                    return Err(Error::FeatureNotSupported(String::from(
                        "UncompressedBuffer cannot be accessed between multiple threads at the same time.",
                    )));
                }

                let decompress_page = {
                    let page = page?;
                    self.was_decompressed = page.is_compressed();
                    // The uncompressed buffer will be take.
                    decompress(page, self.uncompressed_buffer.buffer_mut())?
                };

                Some(decompress_page)
            }
        };

        Ok(())
    }

    #[inline]
    fn get(&self) -> Option<&Self::Item> {
        self.current.as_ref()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<I: Iterator<Item = Result<CompressedPage, Error>>> Drop for BuffedBasicDecompressor<I> {
    fn drop(&mut self) {
        if let Some(page) = self.current.as_mut() {
            let guard = self.uncompressed_buffer.borrow_mut();

            if !std::thread::panicking() && !guard.is_unique_borrow_mut() {
                panic!(
                    "UncompressedBuffer cannot be accessed between multiple threads at the same time."
                );
            }

            {
                let borrow_buffer = self.uncompressed_buffer.buffer_mut();

                if borrow_buffer.capacity() < page.buffer_mut().capacity() {
                    *borrow_buffer = std::mem::take(page.buffer_mut());
                }
            }
        }
    }
}

struct UsedGuard {
    unique_mut: bool,
    inner: Arc<UncompressedBuffer>,
}

impl UsedGuard {
    pub fn create(inner: &Arc<UncompressedBuffer>) -> UsedGuard {
        let used = inner.used.fetch_add(1, Ordering::SeqCst);
        UsedGuard {
            unique_mut: used == 0,
            inner: inner.clone(),
        }
    }
    pub fn is_unique_borrow_mut(&self) -> bool {
        self.unique_mut
    }
}

impl Drop for UsedGuard {
    fn drop(&mut self) {
        self.inner.used.fetch_sub(1, Ordering::SeqCst);
    }
}
