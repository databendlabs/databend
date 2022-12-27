use std::cell::RefCell;
use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::Mutex;

use common_arrow::parquet::error::Error;
use common_arrow::parquet::page::CompressedPage;
use common_arrow::parquet::page::Page;
use common_arrow::parquet::read::decompress;
use common_arrow::parquet::FallibleStreamingIterator;
use common_exception::Result;
use streaming_decompression::Compressed;
use streaming_decompression::Decompressed;

// Note: cannot be accessed between multiple threads at the same time.
pub struct UncompressedBuffer {
    buffer: UnsafeCell<Vec<u8>>,
}

unsafe impl Send for UncompressedBuffer {}

unsafe impl Sync for UncompressedBuffer {}

impl UncompressedBuffer {
    pub fn new(capacity: usize) -> Arc<UncompressedBuffer> {
        Arc::new(UncompressedBuffer {
            buffer: UnsafeCell::new(Vec::with_capacity(capacity)),
        })
    }

    pub fn buffer_mut(&self) -> &mut Vec<u8> {
        unsafe { &mut *self.buffer.get() }
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
                let borrow_buffer = self.uncompressed_buffer.buffer_mut();

                if borrow_buffer.capacity() < page.buffer_mut().capacity() {
                    *borrow_buffer = std::mem::take(page.buffer_mut());
                }
            }
        }

        self.current = match self.iter.next() {
            None => None,
            Some(page) => {
                let page = page?;
                self.was_decompressed = page.is_compressed();
                // The uncompressed buffer will be take.
                Some(decompress(page, self.uncompressed_buffer.buffer_mut())?)
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
            let borrow_buffer = self.uncompressed_buffer.buffer_mut();

            if borrow_buffer.capacity() < page.buffer_mut().capacity() {
                *borrow_buffer = std::mem::take(page.buffer_mut());
            }
        }
    }
}
