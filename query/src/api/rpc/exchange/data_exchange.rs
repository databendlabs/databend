use common_planners::Expression;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum DataExchange {
    Merge(MergeExchange),
    HashDataExchange(HashDataExchange),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HashDataExchange {
    pub destination_ids: Vec<String>,
    pub exchange_expression: Expression,
}

impl HashDataExchange {
    pub fn create(destination_ids: Vec<String>, exchange_expression: Expression) -> DataExchange {
        DataExchange::HashDataExchange(HashDataExchange {
            destination_ids,
            exchange_expression,
        })
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MergeExchange {
    pub destination_id: String,
}

impl MergeExchange {
    pub fn create(destination_id: String) -> DataExchange {
        DataExchange::Merge(MergeExchange { destination_id })
    }
}
