#[cfg(test)]
mod router_test;

mod action_create;
mod action_get;
mod action_list;
mod action_remove;
mod router;

pub use router::ClusterRouter;
