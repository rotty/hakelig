#![type_length_limit = "2097152"]

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use failure::Error;
use futures::sink::Sink;
use futures::{future, stream, Future, Stream};
use structopt::StructOpt;
use tokio::fs;
use tokio::sync::mpsc;
use url::Url;
use regex::Regex;

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
                } else {
                    if predicate(&path) {
                        Box::new(stream::once(Ok(path.into_boxed_path()))) as PathStream
                    } else {
                        Box::new(stream::empty()) as PathStream
                    }
                };
                future::ok(entries)
            })
            .flatten_stream()
    });
    Box::new(entries.flatten())
}

type EntityStream = Box<dyn Stream<Item = Box<dyn Entity>, Error = Error> + Send>;

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
                        .filter_map(|p| classify_path(&p))
                        .map_err(Error::from),
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

fn classify_path(path: &Path) -> Option<Box<dyn Entity>> {
    path.extension()
        .and_then(|ext| ext.to_str())
        .and_then(|ext| match ext {
            "html" | "htm" => match HtmlPath::new(&path) {
                Ok(entity) => Some(Box::new(entity) as Box<Entity>),
                Err(_) => {
                    eprintln!("could not represent filename {} as an URL", path.display());
                    None
                }
            },
            _ => None,
        })
}

#[derive(StructOpt)]
struct Opt {
    root: PathBuf,
    #[structopt(long = "filter-links")]
    link_filter: Option<Regex>,
}

fn main() {
    let opt = Opt::from_args();
    let store = Arc::new(opt.link_filter.map_or_else(Store::new, Store::with_filter));
    let (url_sink, url_source): (mpsc::Sender<Arc<Url>>, _) = mpsc::channel(100);
    let (task_sink, task_source) = mpsc::channel(20);
    let find_roots = list_root(opt.root)
        .map(|entity| future::ok((entity.url(), entity.read_chunks())))
        .buffer_unordered(10)
        .map(move |(url, chunks)| {
            let (chunk_sink, chunk_source) = mpsc::channel(100);
            let tasks = task_sink.clone();
            tasks
                .send(ExtractTask::new(Arc::clone(&url), chunk_source))
                .map_err(Error::from)
                .and_then(|_| chunks.map_err(Error::from).forward(chunk_sink).map(|_| ()))
        })
        .buffer_unordered(10)
        .for_each(|_| future::ok(()))
        .map_err(|e| {
            eprintln!("error: {}", e);
        });
    let task_executor = extraction_thread(Arc::clone(&store), task_source, url_sink);
    let consume = url_source
        .for_each(|_url| {
            //println!("found: {}", url);
            future::ok(())
        })
        .map_err(|_| ());
    let run_both = future::lazy(|| tokio::spawn(consume)).and_then(|_| find_roots);
    tokio::run(run_both);
    task_executor
        .join()
        .expect("joining extractor thread failed");
    for (url, references) in store.lock().known_dangling() {
        //let references: Vec<_> = references.iter().map(|u| u.as_str()).collect();
        //let reference_count = references.count();
        println!("{}:", url);
        for (anchor, referrers) in references {
            match anchor {
                Some(anchor) => println!("  references to {}", anchor),
                None => println!("  document references"),
            }
            for referrer in referrers {
                println!("    {}", referrer);
            }
        }
    }
}
