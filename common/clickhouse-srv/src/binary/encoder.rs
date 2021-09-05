use crate::binary;
use crate::types::Marshal;
use crate::types::StatBuffer;

const MAX_VARINT_LEN64: usize = 10;

#[derive(Default)]
pub struct Encoder {
    buffer: Vec<u8>,
}

impl Encoder {
    pub fn new() -> Self {
        Encoder { buffer: Vec::new() }
    }
    pub fn get_buffer(self) -> Vec<u8> {
        self.buffer
    }

    pub fn get_buffer_ref(&self) -> &[u8] {
        self.buffer.as_ref()
    }

    pub fn uvarint(&mut self, v: u64) {
        let mut scratch = [0u8; MAX_VARINT_LEN64];
        let ln = binary::put_uvarint(&mut scratch[..], v);
        self.write_bytes(&scratch[..ln]);
    }

    pub fn string(&mut self, text: impl AsRef<str>) {
        let bytes = text.as_ref().as_bytes();
        self.byte_string(bytes);
    }

    pub fn byte_string(&mut self, source: impl AsRef<[u8]>) {
        self.uvarint(source.as_ref().len() as u64);
        self.write_bytes(source.as_ref());
    }

    pub fn write<T>(&mut self, value: T)
    where T: Copy + Marshal + StatBuffer {
        let mut buffer = T::buffer();
        value.marshal(buffer.as_mut());
        self.write_bytes(buffer.as_ref());
    }

    pub fn write_bytes(&mut self, b: &[u8]) {
        self.buffer.extend_from_slice(b);
    }
}
