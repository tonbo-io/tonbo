use std::{
    fmt,
    fmt::Debug,
    marker::PhantomData,
    sync::atomic::{AtomicUsize, Ordering},
};

use crate::record::Record;

pub trait Trigger<R: Record>: fmt::Debug {
    fn item(&self, item: &Option<R>) -> bool;

    fn exceeded(&self) -> bool;

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

impl<R: Record> Trigger<R> for SizeOfMemTrigger<R> {
    fn item(&self, item: &Option<R>) -> bool {
        let size = item.as_ref().map_or(0, R::size);
        self.current_size.fetch_add(size, Ordering::SeqCst) + size > self.threshold
    }

    fn exceeded(&self) -> bool {
        self.current_size.load(Ordering::SeqCst) >= self.threshold
    }

    fn reset(&self) {
        self.current_size.store(0, Ordering::SeqCst);
    }
}

#[derive(Debug)]
pub struct CountTrigger {
    threshold: usize,
    count: AtomicUsize,
}

impl CountTrigger {
    pub fn new(threshold: usize) -> Self {
        CountTrigger {
            threshold,
            count: AtomicUsize::new(0),
        }
    }
}

impl<R: Record> Trigger<R> for CountTrigger {
    fn item(&self, _: &Option<R>) -> bool {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.count.load(Ordering::SeqCst) > self.threshold
    }

    fn exceeded(&self) -> bool {
        self.count.load(Ordering::SeqCst) > self.threshold
    }

    fn reset(&self) {
        self.count.store(0, Ordering::SeqCst);
    }
}

#[derive(Copy, Clone, Debug)]
pub enum TriggerType {
    Count,
    SizeOfMem,
}
pub(crate) struct TriggerFactory<R> {
    _p: PhantomData<R>,
}

impl<R: Record> TriggerFactory<R> {
    pub fn create(
        trigger_type: TriggerType,
        threshold: usize,
    ) -> Box<dyn Trigger<R> + Send + Sync> {
        match trigger_type {
            TriggerType::SizeOfMem => Box::new(SizeOfMemTrigger::new(threshold)),
            TriggerType::Count => Box::new(CountTrigger::new(threshold)),
        }
    }
}
