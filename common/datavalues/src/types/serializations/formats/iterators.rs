pub use streaming_iterator::StreamingIterator;

pub struct NullInfo<F>
where F: Fn(usize) -> bool
{
    pub(crate) is_nullable: bool,
    pub(crate) is_null: F,
    pub(crate) null_value: Vec<u8>,
}

impl<F> NullInfo<F>
where F: Fn(usize) -> bool
{
    pub fn new(is_null: F, null_value: Vec<u8>) -> Self {
        Self {
            is_nullable: true,
            is_null,
            null_value,
        }
    }
    pub fn not_nullable(is_null: F) -> Self {
        Self {
            is_nullable: true,
            is_null,
            null_value: vec![],
        }
    }
}

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

pub fn new_it<'a, I, F, T, F2>(
    iterator: I,
    f: F,
    buffer: Vec<u8>,
    nullable: NullInfo<F2>,
) -> Box<dyn StreamingIterator<Item = [u8]> + 'a>
where
    I: Iterator<Item = T> + 'a,
    F: FnMut(T, &mut Vec<u8>) + 'a,
    T: 'a,
    F2: Fn(usize) -> bool + 'a,
{
    if nullable.is_nullable {
        Box::new(NullableBufStreamingIterator::new(
            iterator, f, buffer, nullable,
        ))
    } else {
        Box::new(BufStreamingIterator::new(iterator, f, buffer))
    }
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

pub struct NullableBufStreamingIterator<I, F, F2, T>
where
    I: Iterator<Item = T>,
    F: FnMut(T, &mut Vec<u8>),
    F2: Fn(usize) -> bool,
{
    iterator: I,
    f: F,
    buffer: Vec<u8>,
    is_valid: bool,
    cursor: usize,
    //is_null: F2,
    //null_value: Vec<u8>
    null: NullInfo<F2>,
}

impl<I, F, F2, T> NullableBufStreamingIterator<I, F, F2, T>
where
    I: Iterator<Item = T>,
    F: FnMut(T, &mut Vec<u8>),
    F2: Fn(usize) -> bool,
{
    pub fn new(iterator: I, f: F, buffer: Vec<u8>, null: NullInfo<F2>) -> Self {
        //pub fn new(iterator: I, f: F, buffer: Vec<u8>,  is_null: F2, null_value: Vec<u8>) -> Self {
        Self {
            iterator,
            f,
            buffer,
            is_valid: false,
            null,
            // is_null, null_value,
            cursor: 0,
        }
    }
}

impl<I, F, F2, T> StreamingIterator for NullableBufStreamingIterator<I, F, F2, T>
where
    I: Iterator<Item = T>,
    F: FnMut(T, &mut Vec<u8>),
    F2: Fn(usize) -> bool,
{
    type Item = [u8];

    #[inline]
    fn advance(&mut self) {
        let a = self.iterator.next();
        if let Some(a) = a {
            self.is_valid = true;
            self.buffer.clear();
            if (self.null.is_null)(self.cursor) {
                self.buffer.extend_from_slice(&self.null.null_value)
            } else {
                (self.f)(a, &mut self.buffer);
            }
        } else {
            self.is_valid = false;
        }
        self.cursor += 1;
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
