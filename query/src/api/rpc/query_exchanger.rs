// use std::collections::hash_map::Entry;
// use std::collections::HashMap;
// use std::sync::Arc;
// use common_infallible::RwLock;
// use common_exception::{ErrorCode, Result};
//
// /// One query one data exchanger
// pub struct QueryDataExchange {
//     manager: Arc<QueryDataExchangeManager>,
//     // identifier -> data stream
// }
//
// impl QueryDataExchange {}
//
// pub struct QueryDataExchangeManager {
//     exchanges: RwLock<HashMap<String, QueryDataExchange>>,
// }
//
// impl QueryDataExchangeManager {
//     pub fn create() -> Arc<QueryDataExchangeManager> {
//         Arc::new(QueryDataExchangeManager {
//             exchanges: RwLock::new(HashMap::new()),
//         })
//     }
//
//     pub fn create_exchange(self: &Arc<Self>, query_id: String) -> Result<QueryDataExchange> {
//         let mut exchanges = self.exchanges.write();
//         match exchanges.entry(query_id) {
//             Entry::Occupied(_) => Err(ErrorCode::LogicalError(format!("Query {} already exists data exchange.", query_id))),
//             Entry::Vacant(v) => {
//                 // TODO:
//                 // v.insert()
//             }
//         }
//     }
// }
