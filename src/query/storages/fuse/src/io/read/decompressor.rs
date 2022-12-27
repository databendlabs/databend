use std::cell::RefCell;
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

pub struct BuffedBasicDecompressor<I: Iterator<Item = Result<CompressedPage, Error>>> {
    iter: I,
    current: Option<Page>,
    was_decompressed: bool,
    uncompressed_buffer: Arc<Mutex<Vec<u8>>>,
}

impl<I: Iterator<Item = Result<CompressedPage, Error>>> BuffedBasicDecompressor<I> {
    pub fn new(iter: I, uncompressed_buffer: Arc<Mutex<Vec<u8>>>) -> Self {
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
                let mut borrow_buffer = self.uncompressed_buffer.lock().unwrap();

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
                Some(decompress(
                    page,
                    &mut self.uncompressed_buffer.lock().unwrap(),
                )?)
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
