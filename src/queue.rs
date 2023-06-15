use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

#[derive(Debug, Clone, Default)]
pub struct QueueState(Arc<QueueStateInner>);

#[derive(Debug, Default)]
struct QueueStateInner {
    /// Wether the root-finding process is done
    roots_done: AtomicBool,
    /// How many tasks are queued for extraction
    waiting: AtomicUsize,
    /// How many extraction tasks are currently being processed
    extracting: AtomicUsize,
    /// How many URLs are waiting to be processed
    queued: AtomicUsize,
}

impl QueueState {
    pub fn new() -> Self {
        QueueState::default()
    }
    /// Check whether all work has been done.
    ///
    /// If this returns `true`, the queue is empty, and no further progress can
    /// be made. This condition is used at various points to initiate shutdown,
    /// so the queue state must be carefully manipulated so that this can be
    /// relied on and there are no false positives. This is achieved by, when
    /// issuing multiple operations in a row, always first issuing operations
    /// increasing the size of the queue, and only then those that decrease
    /// it. The rules about this are:
    ///
    /// - Methods named `_enqueued` always increase the queue size.
    ///
    /// - Methods named `_done`, `_dequeued` or `_cancelled` decrease the queue
    ///   size.
    ///
    /// The only exception is `extraction_dequeued`, which can be considered
    /// "neutral", as it increments one internal queue counter, but decrements
    /// another.
    pub fn is_done(&self) -> bool {
        let inner = &self.0;
        inner.roots_done.load(Ordering::SeqCst)
            && inner.waiting.load(Ordering::SeqCst) == 0
            && inner.extracting.load(Ordering::SeqCst) == 0
            && inner.queued.load(Ordering::SeqCst) == 0
    }

    /// Returns whether the root-finding process has finished.
    pub fn roots_are_done(&self) -> bool {
        self.0.roots_done.load(Ordering::SeqCst)
    }

    /// Indicate that the roots-finding proceess has finished.
    pub fn roots_done(&self) {
        self.0.roots_done.store(true, Ordering::SeqCst)
    }

    pub fn waiting_count(&self) -> usize {
        self.0.waiting.load(Ordering::SeqCst)
    }

    pub fn extracting_count(&self) -> usize {
        self.0.extracting.load(Ordering::SeqCst)
    }

    pub fn queued_count(&self) -> usize {
        self.0.queued.load(Ordering::SeqCst)
    }

    /// Indicate a an extraction task has been enqueued.
    pub fn extraction_enqueued(&self) {
        self.0.waiting.fetch_add(1, Ordering::SeqCst);
    }
    /// Indicate that an extration has been started.
    pub fn extraction_dequeued(&self) {
        self.0.extracting.fetch_add(1, Ordering::SeqCst);
        let previous = self.0.waiting.fetch_sub(1, Ordering::SeqCst);
        assert!(previous > 0);
    }
    /// Indicate an extraction has been completed.
    pub fn extraction_done(&self) {
        let previous = self.0.extracting.fetch_sub(1, Ordering::SeqCst);
        assert!(previous > 0);
    }
    /// Indicate an URL has been added to the queue.
    pub fn url_enqueued(&self) {
        self.0.queued.fetch_add(1, Ordering::SeqCst);
    }
    /// Indicate an URL has been taken out of the queue.
    pub fn url_dequeued(&self) {
        let previous = self.0.queued.fetch_sub(1, Ordering::SeqCst);
        assert!(previous > 0);
    }
}
