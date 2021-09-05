#[derive(Clone, Debug)]
pub struct Query {
    sql: String,
    id: String,
}

impl Query {
    pub fn new(sql: impl AsRef<str>) -> Self {
        Self {
            sql: sql.as_ref().to_string(),
            id: "".to_string(),
        }
    }

    pub fn id(self, id: impl AsRef<str>) -> Self {
        Self {
            id: id.as_ref().to_string(),
            ..self
        }
    }

    pub(crate) fn get_sql(&self) -> &str {
        &self.sql
    }

    pub(crate) fn get_id(&self) -> &str {
        &self.id
    }

    pub(crate) fn map_sql<F>(self, f: F) -> Self
    where F: Fn(&str) -> String {
        Self {
            sql: f(&self.sql),
            ..self
        }
    }
}

impl<T> From<T> for Query
where T: AsRef<str>
{
    fn from(source: T) -> Self {
        Self::new(source)
    }
}
