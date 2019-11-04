// Copyright (C) 2019 Andreas Rottmann <mail@r0tty.org>
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use failure::Error;
use futures::{channel::mpsc, prelude::*};
use log::debug;
use regex::RegexSet;
use structopt::StructOpt;
use tokio::{runtime::Runtime, timer};
use url::Url;

mod entity;
mod extract;
mod store;

use entity::{Entity, EntityStream};
use extract::{extraction_thread, ExtractTask};
use store::Store;

#[derive(StructOpt)]
struct Opt {
    roots: Vec<String>,
    #[structopt(long = "link-ignore", number_of_values = 1)]
    link_ignore: Vec<String>,
}

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

struct App {
    backend: Backend,
    // TODO: get rid of `Option` here
    task_source: Option<mpsc::Receiver<ExtractTask>>,
    root_stream: Option<EntityStream>,
    found_source: Option<mpsc::UnboundedReceiver<Arc<Url>>>,
    found_sink: mpsc::UnboundedSender<Arc<Url>>,
    store: Arc<Store>,
    queue_state: QueueState,
}

#[derive(Clone)]
struct Backend {
    task_sink: mpsc::Sender<ExtractTask>,
    store: Arc<Store>,
    queue_state: QueueState,
    entity_ctx: Arc<entity::Context>,
}

async fn submit_chunks(
    url: Arc<Url>,
    mut chunks: entity::ChunkStream,
    mut chunk_sink: mpsc::Sender<Vec<u8>>,
) {
    while let Some(chunk) = chunks.next().await {
        match chunk {
            Ok(chunk) => match chunk_sink.send(chunk).await {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("error submitting chunk for {}: {}", url, e);
                    return;
                }
            },
            Err(e) => {
                eprintln!("error reading chunk for {}: {}", url, e);
                return;
            }
        }
    }
}

async fn submit_entities<S>(mut backend: Backend, mut entities: S)
where
    S: Stream<Item = Result<Box<dyn Entity>, Error>> + Unpin,
{
    while let Some(entity) = entities.next().await {
        let entity = match entity {
            Ok(entity) => entity,
            Err(e) => {
                eprintln!("could not submit entity: {}", e);
                continue;
            }
        };
        let url = entity.url();
        debug!("dequeued {}", url);
        if !backend.store.touch(Arc::clone(&url)) {
            backend.queue_state.url_dequeued();
            debug!("{} already in store", &url);
            continue;
        }
        let url = entity.url();
        debug!("queuing task for {}", &url);
        backend.queue_state.extraction_enqueued();
        backend.queue_state.url_dequeued();
        let chunks = entity.read_chunks(&backend.entity_ctx);
        let (chunk_sink, chunk_source) = mpsc::channel(100);
        debug!("queued {}", url);
        match backend
            .task_sink
            .send(ExtractTask::new(Arc::clone(&url), chunk_source))
            .await
        {
            Ok(_) => {}
            Err(e) => {
                backend.queue_state.extraction_dequeued();
                eprintln!("error submitting task for {}: {}", url, e);
                continue;
            }
        }
        tokio::spawn(submit_chunks(Arc::clone(&url), chunks, chunk_sink));
    }
}

impl App {
    fn from_opt(opt: Opt) -> Result<Self, Error> {
        let link_ignore_set = RegexSet::new(opt.link_ignore)?;
        let (mut root_urls, root_stream) = entity::list_roots(opt.roots)?;
        for url in &mut root_urls {
            url.set_fragment(None);
            url.set_query(None);
            url.path_segments_mut().expect("cannot-be-base URL").pop();
        }
        let store = Arc::new(Store::new(link_ignore_set, Some(root_urls)));
        let (found_sink, found_source) = mpsc::unbounded();
        let (task_sink, task_source) = mpsc::channel(10);
        let entity_ctx = Arc::new(entity::Context::new());
        let queue_state = QueueState::new();
        Ok(App {
            backend: Backend {
                task_sink,
                queue_state: queue_state.clone(),
                store: store.clone(),
                entity_ctx,
            },
            task_source: Some(task_source),
            found_source: Some(found_source),
            root_stream: Some(root_stream),
            found_sink,
            store,
            queue_state,
        })
    }

    fn submit_roots(&self, root_stream: EntityStream) -> impl Future<Output = ()> + 'static {
        let root_enqueue_state = self.queue_state.clone();
        let roots = root_stream.map_ok(move |entity| {
            root_enqueue_state.url_enqueued();
            entity
        });
        let roots_done_state = self.queue_state.clone();
        let backend = self.backend.clone();
        let found_sink = self.found_sink.clone();
        async move {
            submit_entities(backend, roots).await;
            roots_done_state.roots_done();
            if roots_done_state.is_done() {
                found_sink.close_channel();
            }
        }
    }

    async fn submit_found(&self, found_source: mpsc::UnboundedReceiver<Arc<Url>>) {
        let found_state = self.queue_state.clone();
        let backend = self.backend.clone();
        let found = found_source.filter_map(move |url| {
            let found_state = found_state.clone();
            async move {
                match entity::classify_url(&url) {
                    Err(_) => {
                        // TODO: do not swallow error
                        found_state.url_dequeued();
                        None
                    }
                    Ok(entity) => Some(Ok(entity)),
                }
            }
        });
        let found = Box::pin(found);
        submit_entities(backend, found).await;
        debug!("processing new URLs is done");
        if self.queue_state.is_done() {
            let mut task_sink = self.backend.task_sink.clone();
            task_sink.close_channel();
        }
    }

    fn run(&mut self) -> Result<(), Error> {
        let root_stream = self.root_stream.take().unwrap();
        let task_source = self.task_source.take().unwrap();
        let found_source = self.found_source.take().unwrap();
        let submit_roots = self.submit_roots(root_stream);
        let submit_found = self.submit_found(found_source);
        let task_executor = extraction_thread(
            Arc::clone(&self.store),
            task_source,
            self.found_sink.clone(),
            self.queue_state.clone(),
        );
        let state = self.queue_state.clone();
        let queue_state_printer = async move {
            let mut interval = timer::Interval::new_interval(Duration::from_millis(100));
            while let Some(_instant) = interval.next().await {
                eprintln!("queue state: {:?}", state);
                if state.is_done() {
                    break;
                }
            }
        };
        let runtime = Runtime::new()?;
        let run = future::lazy(|_ctx| {
            tokio::spawn(submit_roots);
            tokio::spawn(queue_state_printer);
        })
        .then(|_| submit_found);
        runtime.block_on(run);
        debug!("done running frontend");
        task_executor
            .join()
            .expect("joining extractor thread failed");
        debug!("joined extract thread");
        for (url, references) in self.store.lock().known_dangling() {
            //let references: Vec<_> = references.iter().map(|u| u.as_str()).collect();
            //let reference_count = references.count();
            println!("{}:", url);
            for (anchor, referrers) in references {
                if referrers.is_empty() {
                    continue;
                }
                match anchor {
                    Some(anchor) => println!("  references to {}", anchor),
                    None => println!("  document references"),
                }
                for referrer in referrers {
                    println!("    {}, href {}", referrer.url(), referrer.href());
                }
            }
        }
        Ok(())
    }
}

fn main() -> Result<(), Error> {
    env_logger::init();

    let opt = Opt::from_args();
    let mut app = App::from_opt(opt)?;
    app.run()?;
    Ok(())
}
