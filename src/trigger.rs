use std::{
    marker::PhantomData,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use fusio_log::Encode;

use crate::record::Record;

pub trait FreezeTrigger<R: Record>: Send + Sync {
    fn check_if_exceed(&self, item: &R) -> bool;

    fn reset(&self);
}

#[derive(Debug)]
pub struct SizeOfMemTrigger<R> {
    threshold: usize,
    current_size: AtomicUsize,
    _p: PhantomData<R>,
}

impl<T> SizeOfMemTrigger<T> {
    pub fn new(max_size: usize) -> Self {
        Self {
            threshold: max_size,
            current_size: AtomicUsize::new(0),
            _p: Default::default(),
        }
    }
}

impl<R: Record> FreezeTrigger<R> for SizeOfMemTrigger<R> {
    fn check_if_exceed(&self, item: &R) -> bool {
        let size = item.size() + item.key().size();
        self.current_size.fetch_add(size, Ordering::SeqCst) + size >= self.threshold
    }

    fn reset(&self) {
        self.current_size.store(0, Ordering::SeqCst);
    }
}

#[derive(Debug)]
pub struct LengthTrigger<R> {
    threshold: usize,
    count: AtomicUsize,
    _p: PhantomData<R>,
}

impl<T> LengthTrigger<T> {
    pub fn new(threshold: usize) -> Self {
        Self {
            threshold,
            count: AtomicUsize::new(0),
            _p: Default::default(),
        }
    }
}

impl<R: Record> FreezeTrigger<R> for LengthTrigger<R> {
    fn check_if_exceed(&self, _: &R) -> bool {
        self.count.fetch_add(1, Ordering::SeqCst) + 1 >= self.threshold
    }

    fn reset(&self) {
        self.count.store(0, Ordering::SeqCst);
    }
}

#[derive(Copy, Clone, Debug)]
pub enum TriggerType {
    SizeOfMem(usize),
    #[allow(unused)]
    Length(usize),
}

pub(crate) struct TriggerFactory<R> {
    _p: PhantomData<R>,
}

impl<R: Record> TriggerFactory<R> {
    pub fn create(trigger_type: TriggerType) -> Arc<dyn FreezeTrigger<R>> {
        match trigger_type {
            TriggerType::SizeOfMem(threshold) => Arc::new(SizeOfMemTrigger::new(threshold)),
            TriggerType::Length(threshold) => Arc::new(LengthTrigger::new(threshold)),
        }
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use super::*;
    use crate::tests::Test;

    #[tokio::test]
    async fn test_size_of_mem_trigger() {
        let threshold = 21;
        let trigger = SizeOfMemTrigger::new(threshold);

        let record = Test {
            vstring: "test".to_string(),
            vu32: 0,
            vbool: None,
        };

        let record_size = record.size();
        assert_eq!(record_size, 8);
        let record_size = record.key().size();
        assert_eq!(record_size, 12);

        assert!(
            !trigger.check_if_exceed(&record),
            "Trigger should not be exceeded after 1 record"
        );

        trigger.check_if_exceed(&record);
        assert!(
            trigger.check_if_exceed(&record),
            "Trigger should be exceeded after 2 records"
        );

        trigger.reset();
        assert!(
            !trigger.check_if_exceed(&record),
            "Trigger should not be exceeded after reset"
        );
    }

    #[tokio::test]
    async fn test_length_trigger() {
        let threshold = 2;
        let trigger = LengthTrigger::new(threshold);

        let record = Test {
            vstring: "test".to_string(),
            vu32: 0,
            vbool: None,
        };

        assert!(
            !trigger.check_if_exceed(&record),
            "Trigger should not be exceeded after 1 record"
        );

        trigger.check_if_exceed(&record);
        assert!(
            trigger.check_if_exceed(&record),
            "Trigger should be exceeded after 2 records"
        );

        trigger.reset();
        assert!(
            !trigger.check_if_exceed(&record),
            "Trigger should not be exceeded after reset"
        );
    }
    #[tokio::test]
    async fn test_trigger_factory() {
        let size_of_mem_trigger = TriggerFactory::<Test>::create(TriggerType::SizeOfMem(21));
        let length_trigger = TriggerFactory::<Test>::create(TriggerType::Length(2));

        assert!(!size_of_mem_trigger.check_if_exceed(&Test {
            vstring: "test".to_string(),
            vu32: 0,
            vbool: None
        }));
        assert!(size_of_mem_trigger.check_if_exceed(&Test {
            vstring: "test".to_string(),
            vu32: 0,
            vbool: None
        }));

        assert!(!length_trigger.check_if_exceed(&Test {
            vstring: "test".to_string(),
            vu32: 1,
            vbool: Some(true)
        }));
        assert!(length_trigger.check_if_exceed(&Test {
            vstring: "test".to_string(),
            vu32: 1,
            vbool: Some(true)
        }));
    }
}
