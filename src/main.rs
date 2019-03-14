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

#![type_length_limit = "4194304"]

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use failure::Error;
use futures::sink::Sink;
use futures::{future, Future, Stream};
use log::debug;
use regex::RegexSet;
use structopt::StructOpt;
use tokio::sync::mpsc;
use tokio::timer;

mod entity;
mod extract;
mod store;

use entity::Entity;
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

fn submit_entities<S>(
    entities: S,
    sink: mpsc::Sender<ExtractTask>,
    store: Arc<Store>,
    queue_state: QueueState,
) -> impl Future<Item = (), Error = Error>
where
    S: Stream<Item = Box<dyn Entity>, Error = Error> + Send + 'static,
{
    entities
        .map_err(Error::from)
        .filter_map(move |entity| {
            let url = entity.url();
            debug!("dequeued {}", &url);
            if !store.touch(Arc::clone(&url)) {
                queue_state.url_dequeued();
                debug!("{} already in store", &url);
                return None;
            }
            let chunks = entity.read_chunks();
            let (chunk_sink, chunk_source) = mpsc::channel(100);
            let tasks = sink.clone();
            let log_url = Arc::clone(&url);
            let log_url2 = Arc::clone(&url);
            debug!("queuing task for {}", &url);
            queue_state.extraction_enqueued();
            queue_state.url_dequeued();
            let submit = tasks
                .send(ExtractTask::new(Arc::clone(&url), chunk_source))
                .map(move |f| {
                    debug!("queued {}", log_url);
                    f
                })
                .map_err(Error::from)
                .and_then(move |_| {
                    chunks
                        .map_err(Error::from)
                        .forward(chunk_sink)
                        .map(move |_| {
                            debug!("done sending chunks {}", log_url2);
                        })
                })
                .or_else(move |e| {
                    eprintln!("error processing {}: {}", Arc::clone(&url), e);
                    Ok(())
                });
            Some(submit)
        })
        .for_each(|item| item)
}

fn main() -> Result<(), Error> {
    env_logger::init();

    let opt = Opt::from_args();
    let link_ignore_set = RegexSet::new(opt.link_ignore)?;
    let (mut root_urls, root_stream) = entity::list_roots(opt.roots);
    for url in &mut root_urls {
        url.set_fragment(None);
        url.set_query(None);
        url.path_segments_mut().expect("cannot-be-base URL").pop();
    }
    let store = Arc::new(Store::new(link_ignore_set, Some(root_urls)));
    let (found_sink, found_source) = mpsc::unbounded_channel();
    let (task_sink, task_source) = mpsc::channel(10);
    let state = QueueState::new();
    let root_enqueue_state = state.clone();
    let roots = root_stream.map(move |entity| {
        root_enqueue_state.url_enqueued();
        entity
    });
    let roots_done_state = state.clone();
    let sentinel_sink = task_sink.clone();
    let submit_roots = submit_entities(roots, task_sink.clone(), Arc::clone(&store), state.clone())
        .map_err(|e| {
            eprintln!("could not pass root stream to extractor: {}", e);
        })
        .then(move |_| {
            roots_done_state.roots_done();
            debug!("submitting roots done: {:?}", roots_done_state);
            sentinel_sink
                .send(ExtractTask::sentinel())
                .map_err(|e| {
                    eprintln!("could not send root sentinel: {}", e);
                })
                .map(|_| ())
        });
    let task_executor =
        extraction_thread(Arc::clone(&store), task_source, found_sink, state.clone());
    let found_state = state.clone();
    let found_entities =
        found_source
            .map_err(Error::from)
            .filter_map(move |url| match entity::classify_url(&url) {
                None => {
                    found_state.url_dequeued();
                    None
                }
                Some(entity) => Some(entity),
            });
    let process_extracted_urls =
        submit_entities(found_entities, task_sink, Arc::clone(&store), state.clone())
            .map_err(|e| {
                eprintln!("error processing found URLs: {}", e);
            })
            .then(|result| {
                debug!("processing new URLs is done");
                result
            });
    let queue_state_printer = timer::Interval::new_interval(Duration::from_millis(100))
        .map_err(|e| {
            eprintln!("timer for queue state printer failed: {}", e);
        })
        .for_each(move |_instant| {
            eprintln!("queue state: {:?}", state);
            if state.is_done() {
                return Err(());
            }
            Ok(())
        });
    let run = future::lazy(|| {
        tokio::spawn(submit_roots);
        tokio::spawn(queue_state_printer)
    })
    .and_then(|_| process_extracted_urls);
    tokio::run(run);
    task_executor
        .join()
        .expect("joining extractor thread failed");
    for (url, references) in store.lock().known_dangling() {
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
