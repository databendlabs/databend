use std::time::Instant;

use crate::sessions::SessionRef;

#[derive(PartialEq)]
pub enum ExpiringState {
    InUse,
    Idle { since: Instant },
    Aborted { need_cleanup: bool },
}

pub trait Expirable {
    fn expire_state(&self) -> ExpiringState;
    fn on_expire(&self);
}

impl Expirable for SessionRef {
    fn expire_state(&self) -> ExpiringState {
        if self.is_aborting() {
            ExpiringState::Aborted {
                need_cleanup: false,
            }
        } else if !self.query_context_shared_is_none() {
            ExpiringState::InUse
        } else {
            let status = self.get_status();
            let status = status.read();
            ExpiringState::Idle {
                since: status.last_access(),
            }
        }
    }

    fn on_expire(&self) {}
}
