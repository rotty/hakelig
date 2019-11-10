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

#![warn(rust_2018_idioms)]

use std::{sync::Arc, time::Duration};

use anyhow::Error;
use futures::{channel::mpsc, prelude::*};
use log::debug;
use regex::RegexSet;
use structopt::StructOpt;
use tokio::{runtime::Runtime, timer};
use url::Url;

mod entity;
mod extract;
mod queue;
mod store;

use crate::{
    entity::{Entity, EntityStream},
    extract::{extraction_thread, ExtractTask},
    queue::QueueState,
    store::{FoundUrl, Store},
};

#[derive(StructOpt)]
struct Opt {
    roots: Vec<String>,
    #[structopt(long = "link-ignore", number_of_values = 1)]
    link_ignore: Vec<String>,
    #[structopt(
        short = "r",
        long = "recursion-limit",
        help = "Maximum number of recursions, set to negative to disable limit"
    )]
    recursion_limit: Option<u32>,
}

struct App {
    backend: Backend,
    // TODO: get rid of `Option` here
    task_source: Option<mpsc::Receiver<ExtractTask>>,
    root_stream: Option<EntityStream>,
    found_source: Option<mpsc::UnboundedReceiver<FoundUrl>>,
    found_sink: mpsc::UnboundedSender<FoundUrl>,
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
            if backend.queue_state.is_done() {
                break;
            }
            continue;
        }
        let url = entity.url();
        let found_url = entity.found_url().clone();
        debug!("queuing task for {}", &url);
        // We need to increment the `enqueued` counter first, so that we avoid
        // getting into the `done` state prematurely.
        backend.queue_state.extraction_enqueued();
        backend.queue_state.url_dequeued();
        let read = entity.read(&backend.entity_ctx);
        let chunks = match read.await {
            Ok((mime, chunks)) => {
                match (mime.type_(), mime.subtype()) {
                    (mime::TEXT, mime::HTML) => {}
                    _ => {
                        backend.queue_state.extraction_cancelled();
                        debug!(
                            "{}: ignoring non-HTML entity (MIME type {})",
                            found_url, mime
                        );
                        if backend.queue_state.is_done() {
                            break;
                        }
                        continue;
                    }
                }
                chunks
            }
            Err(e) => {
                eprintln!("failed to read from {}: {}", found_url, e);
                backend.queue_state.extraction_cancelled();
                if backend.queue_state.is_done() {
                    break;
                }
                continue;
            }
        };
        let (chunk_sink, chunk_source) = mpsc::channel(100);
        debug!("queued {}", url);
        match backend
            .task_sink
            .send(ExtractTask::new(found_url, chunk_source))
            .await
        {
            Ok(_) => {}
            Err(e) => {
                backend.queue_state.extraction_cancelled();
                eprintln!("error submitting task for {}: {}", url, e);
                if backend.queue_state.is_done() {
                    break;
                }
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
        let store = Arc::new(Store::new(
            link_ignore_set,
            Some(root_urls),
            opt.recursion_limit,
        ));
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

    fn submit_roots(&self, root_stream: EntityStream) -> impl Future<Output = ()> + 'static + Send {
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

    async fn submit_found(&self, found_source: mpsc::UnboundedReceiver<FoundUrl>) {
        let found_state = self.queue_state.clone();
        let backend = self.backend.clone();
        let found = found_source.filter_map(move |url| {
            let found_state = found_state.clone();
            async move {
                match entity::classify_url(url) {
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
