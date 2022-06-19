use common_planners::Expression;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum DataExchange {
    // None,
    Merge(MergeExchange),
    ShuffleDataExchange(ShuffleDataExchange),
}

impl DataExchange {
    pub fn get_destinations(&self) -> Vec<String> {
        match self {
            // DataExchange::None => vec![],
            DataExchange::Merge(exchange) => vec![exchange.destination_id.clone()],
            DataExchange::ShuffleDataExchange(exchange) => exchange.destination_ids.clone(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ShuffleDataExchange {
    pub destination_ids: Vec<String>,
    pub exchange_expression: Expression,
}

impl ShuffleDataExchange {
    pub fn create(destination_ids: Vec<String>, exchange_expression: Expression) -> DataExchange {
        DataExchange::ShuffleDataExchange(ShuffleDataExchange {
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
