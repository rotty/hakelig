use std::io;
use std::iter;
use std::path::Path;
use std::sync::Arc;

use futures::{future, stream, Future, Stream};
use tokio::fs;
use url::Url;

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
    fn read_links(&self) -> Box<dyn Stream<Item = Url, Error = io::Error> + Send>;
}

struct HtmlPath(Box<Path>);

impl HtmlPath {
    fn new<P: AsRef<Path>>(path: P) -> Self {
        HtmlPath(path.as_ref().into())
    }
}

impl Entity for HtmlPath {
    fn read_links(&self) -> Box<dyn Stream<Item = Url, Error = io::Error> + Send> {
        let links = fs::File::open(self.0.clone())
            .map(|file| stream::empty())
            .flatten_stream();
        Box::new(links)
    }
}

fn classify_path(path: Box<Path>) -> Option<Box<dyn Entity>> {
    path.extension()
        .and_then(|ext| ext.to_str())
        .and_then(|ext| match ext {
            "html" | "htm" => Some(Box::new(HtmlPath::new(&path)) as Box<Entity>),
            _ => None,
        })
}

fn main() {
    let task = dir_lister(".", Arc::new(|_| true))
        .filter_map(classify_path)
        .map(|entity| future::ok(entity.read_links()))
        .buffer_unordered(10)
        .flatten()
        .for_each(|p| {
            println!("found: {}", p);
            future::ok(())
        })
        .map_err(|e| {
            eprintln!("error: {}", e);
        });
    tokio::run(task);
}
