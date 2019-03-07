use std::collections::HashSet;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::thread;

use failure::Error;
use futures::sink::Sink;
use futures::{future, stream, Future, Stream};
use html5ever::tokenizer::{
    BufferQueue, Tag, Token, TokenSink, TokenSinkResult, Tokenizer, TokenizerOpts,
};
use tendril::StrTendril;
use tokio::fs;
use tokio::runtime::current_thread;
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

enum Extracted {
    Url(Url),
    Anchor(Box<str>),
}

// We extract links by feeding chunks of data from a source channel into the
// `html5ever` tokenizer, which is not `Send`. The reason to use a channel and
// not an `AsyncRead` instance directly is that we want to use the tokio
// threadpool runtime for the actual reading, so we can handle blocking disk
// I/O, but cannot run the tokenizer inside the thread pool.
fn extract(source: mpsc::Receiver<Vec<u8>>) -> impl Stream<Item = Extracted, Error = Error> {
    let queue = BufferQueue::new();
    let tokenizer = Tokenizer::new(ExtractSink::default(), TokenizerOpts::default());
    stream::unfold(
        (source, queue, tokenizer, false),
        move |(source, mut queue, mut tokenizer, eof)| {
            if eof {
                None
            } else {
                let read = source.into_future().map(move |(item, rest)| {
                    if let Some(buf) = item {
                        let s = match std::str::from_utf8(&buf) {
                            Ok(s) => s,
                            // TODO: don't swallow error here
                            Err(_) => return (Vec::new(), (rest, queue, tokenizer, true)),
                        };
                        queue.push_back(StrTendril::from_slice(s));
                        let _ = tokenizer.feed(&mut queue); // TODO: check if ignoring result is OK
                        (
                            tokenizer.sink.0.drain(0..).collect(),
                            (rest, queue, tokenizer, false),
                        )
                    } else {
                        (Vec::new(), (rest, queue, tokenizer, true))
                    }
                });
                Some(read)
            }
        },
    )
    .map(|urls| stream::iter_ok(urls))
    .map_err(|(e, _)| e)
    .flatten()
}

#[derive(Default)]
struct ExtractSink(Vec<Extracted>);

impl TokenSink for ExtractSink {
    type Handle = Option<Url>;

    fn process_token(&mut self, token: Token, _line_number: u64) -> TokenSinkResult<Self::Handle> {
        match token {
            // TODO: XML namespace support
            Token::TagToken(Tag {
                ref name,
                ref attrs,
                ..
            }) => {
                if name == "a" {
                    if let Some(href) = attrs.iter().find_map(|attr| {
                        if attr.name.local == *"href" {
                            Url::parse(&attr.value).ok()
                        } else {
                            None
                        }
                    }) {
                        self.0.push(Extracted::Url(href))
                    }
                }
                if let Some(name) = attrs.iter().find_map(|attr| {
                    let name = &attr.name.local;
                    if name == "id" || name == "name" {
                        Some(&attr.value)
                    } else {
                        None
                    }
                }) {
                    self.0.push(Extracted::Anchor(name.to_string().into()))
                }
            }
            _ => {}
        }
        TokenSinkResult::Continue
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

struct ExtractTask {
    url: Arc<Url>,
    chunk_source: mpsc::Receiver<Vec<u8>>,
}

enum Operation {
    SinkUrl(Arc<Url>),
    AddAnchor(Box<str>),
}

fn execute_extraction(
    store: Arc<Store>,
    tasks: mpsc::Receiver<ExtractTask>,
    url_sink: mpsc::Sender<Arc<Url>>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let extract = tasks
            .map(|task| {
                let url = task.url;
                let store = Arc::clone(&store);
                let resolve_store = Arc::clone(&store);
                let resolve_url = Arc::clone(&url);
                let operations =
                    extract(task.chunk_source).filter_map(move |extracted| match extracted {
                        Extracted::Url(link) => {
                            if let Some(unknown_url) =
                                store.add_link(Arc::new(link), Arc::clone(&url))
                            {
                                Some(Operation::SinkUrl(unknown_url))
                            } else {
                                None
                            }
                        }
                        Extracted::Anchor(anchor) => Some(Operation::AddAnchor(anchor)),
                    });
                stream::unfold(
                    (operations, HashSet::new(), false),
                    move |(ops, mut anchors, eof)| {
                        if eof {
                            resolve_store
                                .resolve(Arc::clone(&resolve_url), anchors)
                                .unwrap_or_else(|e| {
                                    eprintln!("could not resolve {}: {}", resolve_url, e)
                                });
                            None
                        } else {
                            let step = ops.into_future().map(move |(item, rest)| {
                                if let Some(op) = item {
                                    let url = match op {
                                        Operation::AddAnchor(anchor) => {
                                            anchors.insert(anchor);
                                            None
                                        }
                                        Operation::SinkUrl(url) => Some(url),
                                    };
                                    (url, (rest, anchors, false))
                                } else {
                                    (None, (rest, anchors, true))
                                }
                            });
                            Some(step)
                        }
                    },
                )
                .filter_map(|item| item)
                .map_err(|(e, _)| e)
            })
            .flatten()
            .fold(url_sink, move |url_sink, link| {
                url_sink.send(link).map_err(Error::from)
            })
            .map(|_url_sink| ());
        current_thread::block_on_all(extract).unwrap_or_else(|e| {
            eprintln!("error extracting links: {}", e);
        });
    })
}

fn main() {
    let store = Arc::new(Store::new());
    let (url_sink, url_source): (mpsc::Sender<Arc<Url>>, _) = mpsc::channel(100);
    let (task_sink, task_source) = mpsc::channel(20);
    let find_roots = dir_lister(".", Arc::new(|_| true))
        .filter_map(classify_path)
        .map(|entity| future::ok((entity.url(), entity.read_chunks())))
        .buffer_unordered(10)
        .map(move |(url, chunks)| {
            let (chunk_sink, chunk_source) = mpsc::channel(100);
            let tasks = task_sink.clone();
            tasks
                .send(ExtractTask {
                    url: Arc::clone(&url),
                    chunk_source,
                })
                .map_err(Error::from)
                .and_then(|_| chunks.map_err(Error::from).forward(chunk_sink).map(|_| ()))
        })
        .map_err(Error::from)
        .buffer_unordered(10)
        .for_each(|_| future::ok(()))
        .map_err(|e| {
            eprintln!("error: {}", e);
        });
    let task_executor = execute_extraction(Arc::clone(&store), task_source, url_sink);
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
    for (url, found, references) in store.lock().dangling() {
        //let references: Vec<_> = references.iter().map(|u| u.as_str()).collect();
        let reference_count = references.iter().count();
        println!("URL {} ({}): {} references", url, found, reference_count);
    }
}
