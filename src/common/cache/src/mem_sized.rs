pub trait MemSized {
    fn mem_bytes(&self) -> usize;
}

impl MemSized for () {
    fn mem_bytes(&self) -> usize {
        0
    }
}

impl MemSized for String {
    fn mem_bytes(&self) -> usize {
        self.len()
    }
}
