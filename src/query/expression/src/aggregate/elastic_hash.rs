#[derive(Debug)]
pub struct Elastic {
    delta: f64,
    c: f64,

    entries: Vec<Option<usize>>,

    batch_limit: usize,
    count: usize,
    i: usize,
    // load_limit: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct Slot(usize);

impl Elastic {
    pub fn with_capacity(n: usize, delta: f64) -> Self {
        assert!(n.is_power_of_two());
        Self {
            entries: vec![None; n],
            c: 1000.0,
            count: 0,
            delta,
            i: 0,
            batch_limit: n / 8 * 3,
            // load_limit: n - (n as f64 * delta) as usize,
        }
    }

    fn levels(&self) -> usize {
        self.entries.len().trailing_zeros() as usize + 1
    }

    fn zone_range(&self, i: usize) -> std::ops::Range<usize> {
        debug_assert!(i >= 1);
        debug_assert!(i <= self.levels());

        let n = self.entries.len();
        let size = n >> (i - 1);
        let start = n - size;
        let end = n - (size >> 1);
        start..end
    }

    fn zone_size(&self, i: usize) -> usize {
        self.entries.len() >> i
    }

    fn zone(&self, i: usize) -> &[Option<usize>] {
        &self.entries[self.zone_range(i)]
    }

    fn cur_batch(&mut self) -> Option<usize> {
        let count = self.count;

        if self.i > self.levels() {
            return None;
        }

        if count < self.batch_limit {
            Some(self.i)
        } else {
            self.i += 1;
            self.batch_limit += self.batch_size(self.i);
            Some(self.i)
        }
    }

    fn batch_size(&self, i: usize) -> usize {
        let n = self.entries.len();
        if i == 0 {
            return n / 8 * 3;
        }
        1.max(((1.0 - self.delta) / 2.0 * (n >> i) as f64) as usize + (n >> (i + 3)))
    }

    fn epsilon(&self, i: usize) -> f64 {
        let r = self.zone_range(i);
        let size = r.len();
        let empty = self.entries[r].iter().filter(|x| x.is_none()).count();
        empty as f64 / size as f64
    }

    fn max_probe(&self, i: usize) -> f64 {
        let t1 = (1.0 / self.epsilon(i)).ln().powi(2);
        let t2 = (1.0 / self.delta).ln();
        self.c * t1.min(t2)
    }

    fn probe_zone(&self, i: usize, mut j: usize, max: Option<usize>) -> Option<usize> {
        let zone = self.zone(i);
        let end = max.map(|max| (j + max) % zone.len());

        let mut c = 0;
        loop {
            c += 1;
            assert!(c < zone.len());
            if zone[j].is_none() {
                return Some(j);
            }

            j += 1;
            if j >= zone.len() {
                j = 0;
            }

            if Some(j) == end {
                return None;
            }
        }
    }

    fn is_case3(&self, i: usize) -> bool {
        let zone = self.zone(i + 1);
        let size = zone.len();
        let empty = zone.iter().filter(|x| x.is_none()).count();
        empty * 4 <= size
    }

    fn is_case1(&self, i: usize) -> bool {
        let zone = self.zone(i);
        let size = zone.len();
        let empty = zone.iter().filter(|x| x.is_none()).count();
        (2 * empty) as f64 > self.delta * size as f64
    }

    pub fn probe(&mut self, value: usize) -> Slot {
        let i = self.cur_batch().unwrap();
        if i == 0 {
            let range = self.zone_range(1);
            let j = value % range.len();
            let j = self.probe_zone(1, j, None).unwrap();
            return Slot(range.start + j);
        }

        if self.is_case3(i) {
            let range = self.zone_range(i);
            let j = value % range.len();
            let j = self.probe_zone(i, j, None).unwrap();
            return Slot(range.start + j);
        }

        if self.is_case1(i) {
            let range = self.zone_range(i);
            let j = value % range.len();
            if let Some(j) = self.probe_zone(i, j, Some(self.max_probe(i) as usize)) {
                return Slot(range.start + j);
            }
        }

        // case 2
        let range = self.zone_range(i + 1);
        let j = value % range.len();
        let j = self.probe_zone(i + 1, j, None).unwrap();
        Slot(range.start + j)
    }

    pub fn mut_entry(&mut self, slot: Slot) -> &mut Option<usize> {
        &mut self.entries[slot.0]
    }

    pub fn insert(&mut self, slot: Slot, value: usize) {
        let x = self.entries[slot.0].replace(value);
        self.count += 1;
        assert!(x.is_none())
    }

    pub fn coord(&self, slot: Slot) -> (usize, usize) {
        for i in 1..self.levels() {
            let range = self.zone_range(i);
            if range.contains(&slot.0) {
                return (i, slot.0 - range.start);
            }
        }
        unreachable!()
    }
}

mod tests {
    use super::*;

    #[test]
    fn test_main() {
        let mut elastic = Elastic::with_capacity(128, 0.2);

        for value in 200..305 {
            println!("start {value}");
            let slot = elastic.probe(value);

            let (i, j) = elastic.coord(slot);
            elastic.insert(slot, value);

            if i != 0 {
                let f = elastic.count as f64 / elastic.entries.len() as f64;
                println!("  probe {i} {j} {f:.3}");
            }
        }

        println!("{elastic:?}")
    }

    #[test]
    fn test_cur_batch() {
        // when i>=1,  B(i).len = A(i).len - A(i).len * delta/2 - 0.75 * A(i).len + 0.75 * A( i+1 ).len
        {
            let mut index = Elastic::with_capacity(128, 0.25);
            let mut ls = vec![];
            while index.cur_batch().is_some() {
                ls.push(index.batch_limit);
                index.count = index.batch_limit;
            }
            assert_eq!(&ls, &[48, 80, 96, 104, 108, 109, 110, 111, 112, 113]);
        }

        {
            let mut index = Elastic::with_capacity(2048, 0.1);
            let mut ls = vec![];
            while index.cur_batch().is_some() {
                ls.push(index.batch_limit);
                index.count = index.batch_limit;
            }
            assert_eq!(&ls, &[
                768, 1356, 1650, 1797, 1870, 1906, 1924, 1933, 1937, 1938, 1939, 1940, 1941, 1942
            ]);
        }
    }
}
