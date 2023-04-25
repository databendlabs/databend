#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateIndexPlan {
    pub catalog: String,
    pub database: String,
    pub table: String,
}
