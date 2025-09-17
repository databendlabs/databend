#[derive(Debug)]
struct Elastic {
    entries: Vec<Option<usize>>,
    count: usize,
    delta: f64,
    c: f64,
}

impl Elastic {
    pub fn with_capacity(n: usize) -> Self {
        assert!(n.is_power_of_two());
        Self {
            entries: vec![None; n],
            c: 1000.0,
            count: 0,
            delta: 0.75,
        }
    }

    fn levels(&self) -> usize {
        // log2(n) + 1
        self.entries.len().trailing_zeros() as usize + 1
    }

    fn ai_range(&self, i: usize) -> std::ops::Range<usize> {
        assert!(i >= 1);
        assert!(i <= self.levels());
        // A_i 大小 = n / 2^(i-1)
        let n = self.entries.len();
        let size = n >> (i - 1);
        let start = n - size;
        let end = n - (size >> 1);
        start..end
    }

    pub fn ai(&self, i: usize) -> &[Option<usize>] {
        let r = self.ai_range(i);
        &self.entries[r]
    }

    pub fn ai_mut(&mut self, i: usize) -> &mut [Option<usize>] {
        let r = self.ai_range(i);
        &mut self.entries[r]
    }

    pub fn epsilon_i(&self, i: usize) -> f64 {
        let r = self.ai_range(i);
        let sz = r.len();
        let empty = self.entries[r].iter().filter(|x| x.is_none()).count();
        empty as f64 / sz as f64
    }

    pub fn next_batch(&self) -> Option<usize> {
        let mut count = self.count;

        for i in 0..self.levels() {
            let b = self.batch_size(i);
            if count > b {
                count -= b
            } else {
                return Some(i);
            }
        }

        None
    }

    pub fn batch_size(&self, i: usize) -> usize {
        if i == 0 {
            return (self.ai_range(1).len() as f64 * 0.75) as usize;
        }

        let ai = self.ai_range(i).len();
        let ai1 = self.ai_range(i + 1).len();

        // 计划批量：Bi (i>=1) = |Ai| - floor(δ|Ai|/2) - ceil(0.75|Ai|) + ceil(0.75|A_{i+1}|)
        let term4 = (0.75_f64 * ai1 as f64).ceil() as usize; // ceil(0.75|A_{i+1}|)
        let term2 = ((self.delta * ai as f64) / 2.0).floor() as usize; // floor(δ|Ai|/2)

        (0.25_f64 * ai as f64).ceil() as usize + term4 - term2
    }

    pub fn f_value(&self, i: usize) -> f64 {
        let t1 = (1.0 / self.epsilon_i(i)).ln().powi(2);
        let t2 = (1.0 / self.delta).ln();
        self.c * t1.min(t2)
    }

    pub fn probe_sub(&self, i: usize, mut j: usize, max: Option<usize>) -> Option<usize> {
        let sub = self.ai(i);
        let end = max.map(|max| (j + max) % sub.len());

        let mut c = 0;
        loop {
            c += 1;
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
            let j = value % self.ai_range(1).len();
            return (0, 1, self.probe_sub(1, j, None).unwrap());
        }

        println!("epsilon_i {} {}", self.epsilon_i(i), self.epsilon_i(i + 1));

        // case 3
        if self.epsilon_i(i + 1) <= 0.25 {
            let j = value % self.ai_range(i).len();
            return (30, i, self.probe_sub(i, j, None).unwrap());
        }

        // case 1
        if self.epsilon_i(i) > 0.5 * self.delta {
            let j = value % self.ai_range(i).len();
            if let Some(j) = self.probe_sub(i, j, Some(self.f_value(i) as usize)) {
                return (10, i, j);
            }
            let j = value % self.ai_range(i + 1).len();
            return (11, i + 1, self.probe_sub(i + 1, j, None).unwrap());
        }

        // case 2
        let j = value % self.ai_range(i + 1).len();
        (20, i + 1, self.probe_sub(i + 1, j, None).unwrap())
    }
}

mod tests {
    use super::*;

    #[test]
    fn test_main() {
        let mut elastic = Elastic::with_capacity(128);

        for value in 200..284 {
            let i = elastic.next_batch().unwrap();
            println!("start {value} {i}");

            let (case, i, j) = elastic.probe(i, value);

            let x = elastic.ai_mut(i)[j].replace(value);
            elastic.count += 1;
            assert_eq!(x, None);

            let f = elastic.count as f64 / elastic.entries.len() as f64;

            if i != 0 {
                println!("  probe {i} {j} {f:.3} {case}");
            }
        }

        println!("{elastic:?}")
    }
}
