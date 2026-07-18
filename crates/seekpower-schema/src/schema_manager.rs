use std::time::{Duration, Instant};

/// 保存触发器
#[derive(Debug)]
pub struct SaveTrigger {
    pending: bool,
    last_save_time: Instant,
    save_interval: Duration,
    pending_schema: Option<crate::Schema>,
}

impl SaveTrigger {
    pub fn new(save_interval: Duration) -> Self {
        Self {
            pending: false,
            last_save_time: Instant::now(),
            save_interval,
            pending_schema: None,
        }
    }

    pub fn mark_dirty(&mut self, schema: crate::Schema) {
        self.pending = true;
        self.pending_schema = Some(schema);
    }

    pub fn should_save(&self) -> bool {
        self.pending && self.last_save_time.elapsed() >= self.save_interval
    }

    pub fn take_pending(&mut self) -> Option<crate::Schema> {
        if self.should_save() {
            self.pending = false;
            self.last_save_time = Instant::now();
            self.pending_schema.take()
        } else {
            None
        }
    }
}

#[cfg(test)]
mod save_trigger_tests {
    use super::*;

    #[test]
    fn test_save_trigger_initial_state() {
        let mut trigger = SaveTrigger::new(Duration::from_millis(100));
        assert!(!trigger.should_save());
        assert!(trigger.take_pending().is_none());
    }

    #[test]
    fn test_save_trigger_mark_and_wait() {
        let mut trigger = SaveTrigger::new(Duration::from_millis(10));
        let schema = crate::Schema::new();
        trigger.mark_dirty(schema);
        assert!(!trigger.should_save()); // 时间未到
        std::thread::sleep(Duration::from_millis(20));
        assert!(trigger.should_save());
    }
}
