// put_uvarint encodes a uint64 into buf and returns the number of bytes written.
// If the buffer is too small, put_uvarint will panic.
pub fn put_uvarint(mut buffer: impl AsMut<[u8]>, x: u64) -> usize {
    let mut i = 0;
    let mut mx = x;
    let buf = buffer.as_mut();
    while mx >= 0x80 {
        buf[i] = mx as u8 | 0x80;
        mx >>= 7;
        i += 1;
    }
    buf[i] = mx as u8;
    i + 1
}

#[cfg(test)]
mod test {
    #[test]
    fn test_put_uvarint() {
        let expected = [148u8, 145, 6, 0, 0, 0, 0, 0, 0, 0];
        let mut buffer = [0u8; 10];

        let actual = super::put_uvarint(&mut buffer[..], 100_500);

        assert_eq!(actual, 3);
        assert_eq!(buffer, expected);
    }
}
