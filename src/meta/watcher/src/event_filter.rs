// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;

/// Bitmask filter for watcher event types.
///
/// This structure allows fine-grained control over which types of events (e.g., update, delete)
/// a watcher should receive notifications for. Use the constructors `update()`, `delete()`, or `all()`
/// for common filtering configurations.
///
/// This design provides flexibility in specifying the specific event types that a watcher should receive.
#[derive(Clone, Debug, Copy)]
pub struct EventFilter(u64);

impl fmt::Display for EventFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut written = false;

        if self.accepts_update() {
            write!(f, "update")?;
            written = true;
        }

        if self.accepts_delete() {
            if written {
                write!(f, "|")?;
            }
            write!(f, "delete")?;
        }

        Ok(())
    }
}

impl EventFilter {
    pub const UPDATE: u64 = 0x1;
    pub const DELETE: u64 = 0x2;

    pub fn all() -> Self {
        Self(Self::UPDATE | Self::DELETE)
    }

    pub fn update() -> Self {
        Self(Self::UPDATE)
    }

    pub fn delete() -> Self {
        Self(Self::DELETE)
    }

    /// If the filter accept an update event
    pub fn accepts_update(&self) -> bool {
        self.0 & Self::UPDATE != 0
    }

    /// If the filter accept a delete event
    pub fn accepts_delete(&self) -> bool {
        self.0 & Self::DELETE != 0
    }

    /// Checks if the filter accepts the specified event type
    pub fn accepts_event_type(&self, event_type: u64) -> bool {
        self.0 & event_type != 0
    }
}

#[cfg(test)]
mod tests {
    use super::EventFilter;

    #[test]
    fn test_event_filter() {
        let filter = EventFilter::all();
        assert!(filter.accepts_update());
        assert!(filter.accepts_delete());

        let filter = EventFilter::update();
        assert!(filter.accepts_update());
        assert!(!filter.accepts_delete());

        let filter = EventFilter::delete();
        assert!(!filter.accepts_update());
        assert!(filter.accepts_delete());
    }

    #[test]
    fn test_accepts_event_type() {
        let filter = EventFilter::all();
        assert!(filter.accepts_event_type(EventFilter::UPDATE));
        assert!(filter.accepts_event_type(EventFilter::DELETE));

        let filter = EventFilter::update();
        assert!(filter.accepts_event_type(EventFilter::UPDATE));
        assert!(!filter.accepts_event_type(EventFilter::DELETE));

        let filter = EventFilter::delete();
        assert!(!filter.accepts_event_type(EventFilter::UPDATE));
        assert!(filter.accepts_event_type(EventFilter::DELETE));

        // Test with custom event type
        let custom_event = 0x4;
        assert!(!filter.accepts_event_type(custom_event));

        // Test with combined event types
        let combined_filter = EventFilter(EventFilter::UPDATE | EventFilter::DELETE);
        assert!(combined_filter.accepts_event_type(EventFilter::UPDATE));
        assert!(combined_filter.accepts_event_type(EventFilter::DELETE));
    }

    #[test]
    fn test_display() {
        let filter = EventFilter::all();
        assert_eq!(format!("{}", filter), "update|delete");

        let filter = EventFilter::update();
        assert_eq!(format!("{}", filter), "update");

        let filter = EventFilter::delete();
        assert_eq!(format!("{}", filter), "delete");
    }
}
