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

use std::{env, path::Path, sync::Arc, time::Duration};

use futures::{future, StreamExt};
use log::debug;
use once_cell::sync::Lazy;
use regex::{Regex, RegexSet};
use structopt::StructOpt;
use tokio::{
    runtime::Runtime,
    signal,
    sync::{broadcast, mpsc},
};
use url::Url;

mod entity;
mod extract;
mod queue;
mod store;

use entity::Entity;
use extract::{extraction_thread, ExtractTask};
use queue::QueueState;
use store::{FoundUrl, Store};

#[derive(StructOpt)]
struct Opt {
    roots: Vec<String>,
    #[structopt(long = "link-ignore", number_of_values = 1)]
    link_ignore: Vec<String>,
    #[structopt(
        short = "r",
        long = "recursion-limit",
        help = "Maximum number of recursions"
    )]
    recursion_limit: Option<u32>,
}

static URI_SCHEME: Lazy<Regex> = Lazy::new(|| Regex::new("^[a-z]+://").unwrap());

fn parse_root<T: AsRef<str>>(root: T) -> anyhow::Result<Url> {
    let root = root.as_ref();
    if URI_SCHEME.is_match(root) {
        Ok(Url::parse(root)?)
    } else {
        let path = Path::new(root);
        if path.is_relative() {
            let mut absolute = env::current_dir()?;
            absolute.push(path);
            Ok(Url::from_file_path(absolute).unwrap())
        } else {
            Ok(Url::from_file_path(path).unwrap())
        }
    }
}

#[derive(Clone)]
struct Backend {
    task_sink: async_channel::Sender<ExtractTask>,
    store: Arc<Store>,
    queue_state: QueueState,
    entity_ctx: Arc<entity::Context>,
    done_tx: broadcast::Sender<()>,
}

impl Backend {
    async fn submit_entity(&self, entity: Box<dyn Entity>) -> anyhow::Result<()> {
        let url = entity.url();
        debug!("dequeued {}", &url);
        if !self.store.touch(Arc::clone(&url)) {
            self.queue_state.url_dequeued();
            debug!("{} already in store", &url);
            return Ok(());
        }
        let entity_ctx = self.entity_ctx.clone();
        let mut chunks = match entity.read(entity_ctx).await {
            Ok((mime, chunks)) => {
                if mime != mime::TEXT_HTML && mime != mime::TEXT_HTML_UTF_8 {
                    debug!("{}: ignoring non-HTML entity (MIME type {})", url, mime);
                    self.queue_state.url_dequeued();
                    return Ok(());
                }
                chunks
            }
            Err(e) => {
                self.queue_state.url_dequeued();
                eprintln!("cannot read from {}: {}", url, e);
                return Ok(());
            }
        };
        // We need to increment the `enqueued` counter first, so that we avoid
        // getting into the `done` state prematurely.
        self.queue_state.extraction_enqueued();
        self.queue_state.url_dequeued();
        let (chunk_sink, chunk_source) = mpsc::channel(100);
        let tasks = self.task_sink.clone();
        debug!("queuing task for {}", &url);
        tasks
            .send(ExtractTask::new(entity.found_url().clone(), chunk_source))
            .await?;
        debug!("queued {}", url);
        //tokio::pin!(chunks);
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk?;
            chunk_sink.send(chunk).await?;
        }
        debug!("done sending chunks {}", url);
        Ok(())
    }

    async fn submit_roots(&self, root_urls: Vec<Url>) -> anyhow::Result<()> {
        let mut root_list = entity::list_roots(&root_urls);
        let mut done_rx = self.done_tx.subscribe();
        loop {
            let item = tokio::select! {
                item = root_list.next() => {
                    match item.transpose() {
                        Some(item) => item,
                        None => break,
                    }
                }
                _ = done_rx.recv() => break,
            };
            let root = match item {
                Err(e) => {
                    eprintln!("Error listing roots: {}", e);
                    continue;
                }
                Ok(root) => store::FoundUrl(Arc::new(root), 0),
            };
            let entity = match entity::classify_url(root.clone()) {
                Ok(entity) => entity,
                Err(e) => {
                    // TODO: do not swallow error
                    debug!("could not classify root {}: {}", root, e);
                    continue;
                }
            };
            self.queue_state.url_enqueued();
            if let Err(e) = self.submit_entity(entity).await {
                eprintln!("Error submitting roots: {}", e);
            }
        }
        self.queue_state.roots_done();
        debug!("Submitting roots done: {:?}", self.queue_state);
        if self.queue_state.is_done() {
            if let Err(e) = self.done_tx.send(()) {
                debug!("Root submission failed tp notify completion: {}", e);
            }
        }
        Ok(())
    }

    async fn process_extracted_urls(
        &self,
        mut url_source: mpsc::UnboundedReceiver<FoundUrl>,
    ) -> anyhow::Result<()> {
        let mut done_rx = self.done_tx.subscribe();
        loop {
            if self.queue_state.is_done() {
                if let Err(e) = self.done_tx.send(()) {
                    debug!("Root submission failed tp notify completion: {}", e);
                }
                break;
            }
            let url = tokio::select! {
                url = url_source.recv() => {
                    match url {
                        Some(url) => url,
                        None => break,
                    }
                }
                _ = done_rx.recv() => break,
            };
            let entity = match entity::classify_url(url.clone()) {
                Ok(entity) => entity,
                Err(_) => {
                    // TODO: do not swallow error
                    self.queue_state.url_dequeued();
                    continue;
                }
            };
            if let Err(e) = self.submit_entity(entity).await {
                debug!("Failed to resolve entity {}: {}", url, e);
                self.store
                    .resolve_error(Arc::clone(&url.0), e.to_string())
                    .unwrap_or_else(|e| {
                        eprintln!("could not record failure for {}: {}", url, e);
                    });
            }
        }
        Ok(())
    }
}

async fn print_queue_state(state: QueueState, mut done_rx: broadcast::Receiver<()>) {
    let mut timer = tokio::time::interval(Duration::from_millis(100));

    loop {
        eprintln!("queue state: {:?}", state);
        tokio::select! {
            _ = timer.tick() => {},
            _ = done_rx.recv() => break,
        }
    }
}

async fn signal_handler(done_tx: broadcast::Sender<()>) -> anyhow::Result<()> {
    signal::ctrl_c().await?;
    debug!("Received Ctrl-C");
    done_tx.send(())?;
    Ok(())
}

fn main() -> anyhow::Result<()> {
    env_logger::init();

    let opt = Opt::from_args();
    let link_ignore_set = RegexSet::new(opt.link_ignore)?;
    let root_urls = opt
        .roots
        .iter()
        .map(parse_root)
        .collect::<anyhow::Result<Vec<Url>>>()?;
    let mut parent_urls = root_urls.clone();
    for url in &mut parent_urls {
        url.set_fragment(None);
        url.set_query(None);
        url.path_segments_mut().expect("cannot-be-base URL").pop();
    }
    let store = Arc::new(Store::new(
        link_ignore_set,
        Some(parent_urls),
        opt.recursion_limit,
    ));
    let queue_state = QueueState::new();
    let (found_sink, found_source) = mpsc::unbounded_channel();
    let (task_sink, task_source) = async_channel::bounded(10);
    let entity_ctx = Arc::new(entity::Context::new());
    let rt = Arc::new(Runtime::new()?);
    let (done_tx, _) = broadcast::channel(1);
    let backend = Backend {
        task_sink,
        queue_state: queue_state.clone(),
        store: Arc::clone(&store),
        entity_ctx,
        done_tx: done_tx.clone(),
    };
    let root_submitter = backend.submit_roots(root_urls);
    let extracted_processor = backend.process_extracted_urls(found_source);
    debug!("Spawning {} extraction threads", num_cpus::get());
    let task_executors: Vec<_> = (0..num_cpus::get())
        .map(|i| {
            debug!("Spawning extraction thread {}", i);
            extraction_thread(
                Arc::clone(&rt),
                Arc::clone(&store),
                task_source.clone(),
                done_tx.clone(),
                found_sink.clone(),
                queue_state.clone(),
            )
        })
        .collect();
    rt.spawn(print_queue_state(queue_state, done_tx.subscribe()));
    rt.spawn(async move {
        if let Err(e) = signal_handler(done_tx.clone()).await {
            eprintln!("Error in signal handler: {}", e);
        }
    });
    if let Err(e) = rt.block_on(future::try_join(root_submitter, extracted_processor)) {
        eprintln!("Submitting roots failed: {}", e);
    }

    for (i, executor) in task_executors.into_iter().enumerate() {
        if let Err(e) = executor.join().expect("joining extractor thread failed") {
            eprintln!("Task executor thread {} failed: {}", i, e);
        }
        debug!("Extraction thread {} completed successfully", i);
    }

    for (url, references) in store.lock().known_dangling() {
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
