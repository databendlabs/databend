pub mod csv;
pub mod iterators;

#[inline]
pub fn lexical_to_bytes_mut_no_clear<N: lexical_core::ToLexical>(n: N, buf: &mut Vec<u8>) {
    buf.reserve(N::FORMATTED_SIZE_DECIMAL);
    let len0 = buf.len();
    unsafe {
        // JUSTIFICATION
        //  Benefit
        //      Allows using the faster serializer lexical core and convert to string
        //  Soundness
        //      Length of buf is set as written length afterwards. lexical_core
        //      creates a valid string, so doesn't need to be checked.
        let slice =
            std::slice::from_raw_parts_mut(buf.as_mut_ptr().add(len0), buf.capacity() - len0);
        let len = lexical_core::write(n, slice).len();
        buf.set_len(len0 + len);
    }
}
