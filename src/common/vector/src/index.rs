use ndarray::ArrayViewMut;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum VectorIndex {
    IvfFlat(IvfFlatIndex),
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IvfFlatIndex {
    pub nlists: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MetricType {
    Cosine,
}

pub fn normalize(vec: &mut [f32]) {
    const DIFF: f32 = 1e-6;
    let mut vec = ArrayViewMut::from(vec);
    let norm = vec.dot(&vec).sqrt();
    if (norm - 1.0).abs() < DIFF || (norm - 0.0).abs() < DIFF {
        return;
    }
    vec.mapv_inplace(|x| x / norm);
}
