#[cfg(test)]
mod router_test;

mod router;
mod action_create;
mod action_list;
mod action_get;
mod action_remove;

pub use router::ClusterRouter;
