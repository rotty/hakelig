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
    pub fn is_done(&self) -> bool {
        let inner = &self.0;
        inner.roots_done.load(Ordering::SeqCst)
            && inner.waiting.load(Ordering::SeqCst) == 0
            && inner.extracting.load(Ordering::SeqCst) == 0
            && inner.queued.load(Ordering::SeqCst) == 0
    }
    pub fn roots_done(&self) {
        self.0.roots_done.store(true, Ordering::SeqCst)
    }
    pub fn extraction_enqueued(&self) {
        self.0.waiting.fetch_add(1, Ordering::SeqCst);
    }
    pub fn extraction_dequeued(&self) {
        self.0.extracting.fetch_add(1, Ordering::SeqCst);
        let previous = self.0.waiting.fetch_sub(1, Ordering::SeqCst);
        assert!(previous > 0);
    }
    pub fn extraction_done(&self) {
        let previous = self.0.extracting.fetch_sub(1, Ordering::SeqCst);
        assert!(previous > 0);
    }
    pub fn url_enqueued(&self) {
        self.0.queued.fetch_add(1, Ordering::SeqCst);
    }
    pub fn url_dequeued(&self) {
        let previous = self.0.queued.fetch_sub(1, Ordering::SeqCst);
        assert!(previous > 0);
    }
}
