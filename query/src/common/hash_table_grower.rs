#[derive(Clone)]
pub struct Grower {
    size_degree: u8
}

impl Default for Grower
{
    fn default() -> Self {
        Grower {
            size_degree: 8
        }
    }
}

impl Grower {
    #[inline]
    pub fn max_size(&self) -> isize {
        1_isize << self.size_degree
    }

    #[inline]
    pub fn overflow(&self, size: usize) -> bool {
        size > ((1_usize) << (self.size_degree - 1))
    }

    #[inline]
    pub fn place(&self, hash_value: u64) -> isize {
        hash_value as isize & (((1_isize) << self.size_degree) - 1)
    }

    #[inline]
    pub fn next_place(&self, old_place: isize) -> isize {
        (old_place + 1) & ((1_isize << self.size_degree) - 1)
    }

    #[inline]
    pub fn increase_size(&mut self) {
        self.size_degree += if self.size_degree >= 23 { 1 } else { 2 };
    }
}
