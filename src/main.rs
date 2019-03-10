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

use std::io;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use failure::Error;
use futures::sink::Sink;
use futures::{future, stream, Future, Stream};
use log::debug;
use regex::RegexSet;
use structopt::StructOpt;
use tokio::fs;
use tokio::sync::mpsc;
use tokio::timer;
use url::Url;

mod extract;
mod store;

use extract::{extraction_thread, ExtractTask};
use store::Store;

type PathStream = Box<dyn Stream<Item = Box<Path>, Error = io::Error> + Send>;

fn dir_lister<P>(path: P, predicate: Arc<dyn Fn(&Path) -> bool + Send + Sync>) -> PathStream
where
    P: AsRef<Path> + Send + Sync + 'static,
{
    let entries = fs::read_dir(path).flatten_stream().map(move |entry| {
        let path = entry.path();
        let predicate = predicate.clone();
        future::poll_fn(move || entry.poll_file_type())
            .and_then(move |ft| {
                let entries = if ft.is_dir() {
                    dir_lister(path, predicate)
                } else if predicate(&path) {
                    Box::new(stream::once(Ok(path.into_boxed_path()))) as PathStream
                } else {
                    Box::new(stream::empty()) as PathStream
                };
                future::ok(entries)
            })
            .flatten_stream()
    });
    Box::new(entries.flatten())
}

type EntityStream = Box<dyn Stream<Item = Box<dyn Entity + Send>, Error = Error> + Send>;

fn list_root<P>(path: P) -> EntityStream
where
    P: AsRef<Path> + Send + Sync + 'static,
{
    let inner_path = path.as_ref().to_owned();
    let stream = fs::metadata(path)
        .map(|metadata| {
            let path = inner_path;
            if metadata.file_type().is_dir() {
                Box::new(
                    dir_lister(path, Arc::new(|_| true))
                        .map_err(Error::from)
                        .filter_map(|p| classify_path(&p)),
                ) as EntityStream
            } else {
                Box::new(stream::once(Ok(path)).filter_map(|p| classify_path(p.as_ref())))
                    as EntityStream
            }
        })
        .map_err(Error::from)
        .flatten_stream();
    Box::new(stream)
}

trait Entity {
    fn url(&self) -> Arc<Url>;
    fn read_chunks(&self) -> Box<dyn Stream<Item = Vec<u8>, Error = io::Error> + Send>;
}

struct HtmlPath {
    path: Box<Path>,
    url: Arc<Url>,
}

impl HtmlPath {
    fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let mut abs_path = std::env::current_dir()?;
        abs_path.push(path);
        let url = Url::from_file_path(&abs_path).expect("unrepresentable path");
        Ok(HtmlPath {
            path: abs_path.into(),
            url: Arc::new(url),
        })
    }
}

impl Entity for HtmlPath {
    fn url(&self) -> Arc<Url> {
        self.url.clone()
    }
    fn read_chunks(&self) -> Box<dyn Stream<Item = Vec<u8>, Error = io::Error> + Send> {
        let buf = vec![0u8; 4096];
        let links = fs::File::open(self.path.clone())
            .map(|file| {
                stream::unfold((file, buf, false), |(file, buf, eof)| {
                    if eof {
                        None
                    } else {
                        let read = tokio::io::read(file, buf).map(|(file, buf, n_read)| {
                            if n_read == 0 {
                                (Vec::new(), (file, buf, true))
                            } else {
                                (Vec::from(&buf[..n_read]), (file, buf, false))
                            }
                        });
                        Some(read)
                    }
                })
            })
            .flatten_stream();
        Box::new(links)
    }
}

fn classify_path(path: &Path) -> Option<Box<dyn Entity + Send>> {
    path.extension()
        .and_then(|ext| ext.to_str())
        .and_then(|ext| match ext {
            "html" | "htm" => match HtmlPath::new(&path) {
                Ok(entity) => Some(Box::new(entity) as Box<Entity + Send>),
                Err(_) => {
                    eprintln!("could not represent filename {} as an URL", path.display());
                    None
                }
            },
            _ => None,
        })
}

fn classify_url(url: &Url) -> Option<Box<dyn Entity + Send>> {
    match url.scheme() {
        "file" => url
            .to_file_path()
            .map_err(|_| {
                eprintln!("could not construct file path from URL {}", url);
            })
            .ok()
            .and_then(|path| classify_path(&path)),
        _ => None,
    }
}

#[derive(StructOpt)]
struct Opt {
    root: PathBuf,
    #[structopt(long = "link-ignore")]
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
    S: Stream<Item = Box<dyn Entity + Send>, Error = Error> + Send + 'static,
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
    let store = Arc::new(Store::new(link_ignore_set));
    let (found_sink, found_source) = mpsc::unbounded_channel();
    let (task_sink, task_source) = mpsc::channel(10);
    let state = QueueState::new();
    let root_enqueue_state = state.clone();
    let roots = list_root(opt.root).map(move |entity| {
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
            .filter_map(move |url| match classify_url(&url) {
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
