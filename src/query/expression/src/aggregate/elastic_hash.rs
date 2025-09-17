#[derive(Debug)]
struct Elastic {
    entries: Vec<Option<usize>>,
    count: usize,
    delta: f64,
    c: f64,
    i: usize,
    batch_limit: usize,
    load_limit: usize,
}

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
            load_limit: n - (n as f64 * delta) as usize,
        }
    }

    fn levels(&self) -> usize {
        // log2(n) + 1
        self.entries.len().trailing_zeros() as usize + 1
    }

    fn sub_range(&self, i: usize) -> std::ops::Range<usize> {
        debug_assert!(i >= 1);
        debug_assert!(i <= self.levels());

        let n = self.entries.len();
        let size = n >> (i - 1);
        let start = n - size;
        let end = n - (size >> 1);
        start..end
    }

    fn sub_size(&self, i: usize) -> usize {
        self.entries.len() >> i
    }

    pub fn sub(&self, i: usize) -> &[Option<usize>] {
        let r = self.sub_range(i);
        &self.entries[r]
    }

    pub fn sub_mut(&mut self, i: usize) -> &mut [Option<usize>] {
        let r = self.sub_range(i);
        &mut self.entries[r]
    }

    pub fn epsilon_i(&self, i: usize) -> f64 {
        let r = self.sub_range(i);
        let sz = r.len();
        let empty = self.entries[r].iter().filter(|x| x.is_none()).count();
        empty as f64 / sz as f64
    }

    pub fn cur_batch(&mut self) -> Option<usize> {
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

    pub fn f_value(&self, i: usize) -> f64 {
        let t1 = (1.0 / self.epsilon_i(i)).ln().powi(2);
        let t2 = (1.0 / self.delta).ln();
        self.c * t1.min(t2)
    }

    pub fn probe_sub(&self, i: usize, mut j: usize, max: Option<usize>) -> Option<usize> {
        let sub = self.sub(i);
        let end = max.map(|max| (j + max) % sub.len());

        let mut c = 0;
        loop {
            c += 1;
            if c > sub.len() {
                panic!()
            }
            if sub[j].is_none() {
                println!("    probe count {c}");
                return Some(j);
            }

            j += 1;
            if j >= sub.len() {
                j = 0;
            }

            if Some(j) == end {
                println!("probe count {c}");
                return None;
            }
        }
    }

    pub fn probe(&self, i: usize, value: usize) -> (u8, usize, usize) {
        if i == 0 {
            let j = value % self.sub_size(1);
            return (0, 1, self.probe_sub(1, j, None).unwrap());
        }

        println!("epsilon_i {} {}", self.epsilon_i(i), self.epsilon_i(i + 1));

        // case 3
        if self.epsilon_i(i + 1) <= 0.25 {
            let j = value % self.sub_size(i);
            return (30, i, self.probe_sub(i, j, None).unwrap());
        }

        // case 1
        if self.epsilon_i(i) > 0.5 * self.delta {
            let j = value % self.sub_size(i);
            if let Some(j) = self.probe_sub(i, j, Some(self.f_value(i) as usize)) {
                return (10, i, j);
            }
            let j = value % self.sub_size(i + 1);
            return (11, i + 1, self.probe_sub(i + 1, j, None).unwrap());
        }

        // case 2
        let j = value % self.sub_size(i + 1);
        (20, i + 1, self.probe_sub(i + 1, j, None).unwrap())
    }
}

mod tests {
    use super::*;

    #[test]
    fn test_main() {
        let mut elastic = Elastic::with_capacity(128, 0.75);

        for value in 200..280 {
            let i = elastic.cur_batch().unwrap();
            println!("start {value} {i}");

            let (case, i, j) = elastic.probe(i, value);

            let x = elastic.sub_mut(i)[j].replace(value);
            elastic.count += 1;
            assert_eq!(x, None);

            let f = elastic.count as f64 / elastic.entries.len() as f64;

            if i != 0 {
                println!("  probe {i} {j} {f:.3} {case}");
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
