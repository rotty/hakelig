use std::collections::HashSet;
use std::io;
use std::path::Path;
use std::sync::Arc;

use failure::Error;
use futures::{future, stream, Future, Stream};
use futures::sink::Sink;
use tokio::fs;
use tokio::sync::mpsc;
use url::Url;

mod store;

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

trait Entity {
    fn url(&self) -> Arc<Url>;
    fn read_links(&self) -> Box<dyn Stream<Item = Url, Error = io::Error> + Send>;
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
    fn read_links(&self) -> Box<dyn Stream<Item = Url, Error = io::Error> + Send> {
        let links = fs::File::open(self.path.clone())
            .map(|file| stream::empty())
            .flatten_stream();
        Box::new(links)
    }
}

fn classify_path(path: Box<Path>) -> Option<Box<dyn Entity>> {
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

type UrlStream = Box<dyn Stream<Item = Arc<Url>, Error = io::Error> + Send>;

fn fetch_links(store: Arc<Store>, url: Arc<Url>, referrer: Arc<Url>) -> UrlStream {
    Box::new(stream::once(Ok(url)))
}

fn main() {
    let store = Arc::new(Store::new());
    let (url_sink, url_source): (mpsc::Sender<Arc<Url>>, _) = mpsc::channel(100);
    let find_store = Arc::clone(&store);
    let find_roots = dir_lister(".", Arc::new(|_| true))
        .filter_map(classify_path)
        .map(|entity| future::ok((entity.url(), entity.read_links())))
        .buffer_unordered(10)
        .map(move |(url, links)| {
            let store = Arc::clone(&find_store);
            store
                .resolve(Arc::clone(&url), HashSet::new())
                .unwrap_or_else(|e| eprintln!("could not resolve {}: {}", url, e));
            links.map(move |link| {
                if let Some(unknown_url) = store.add_link(Arc::new(link), Arc::clone(&url)) {
                    future::ok(fetch_links(Arc::clone(&store), unknown_url, Arc::clone(&url)))
                } else {
                    future::ok(Box::new(stream::empty()) as UrlStream)
                }
            })
        })
        .flatten()
        .buffer_unordered(10)
        .flatten()
        .map_err(Error::from)
        .forward(url_sink)
        .map(|(_, _)| ())
        .map_err(|e| {
            eprintln!("error: {}", e);
        });
    let consume = url_source.for_each(|url| {
        println!("found: {}", url);
        future::ok(())
    }).map_err(|_| ());
    let run_both = future::lazy(|| tokio::spawn(consume)).and_then(|_| find_roots);
    tokio::run(run_both);
    for (url, found, references) in store.lock().dangling() {
        let references: Vec<_> = references.iter().map(|u| u.as_str()).collect();
        println!("URL {} ({}): [{}]", url, found, references.join(", "));
    }
}
