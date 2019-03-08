#![type_length_limit = "2097152"]

use std::collections::HashSet;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::str;
use std::sync::Arc;
use std::thread;

use failure::Error;
use futures::sink::Sink;
use futures::{future, stream, Future, IntoFuture, Stream};
use html5ever::tokenizer::{
    BufferQueue, Tag, Token, TokenSink, TokenSinkResult, Tokenizer, TokenizerOpts, TokenizerResult,
};
use html5ever::Attribute;
use lazy_static::lazy_static;
use regex::Regex;
use structopt::StructOpt;
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

enum Extracted {
    Url(Url),
    Anchor(Box<str>),
}

enum ExtractError {
    Redirect(Url),
    Recv(mpsc::error::RecvError),
    Send(mpsc::error::SendError),
    Utf8(str::Utf8Error),
}

impl From<mpsc::error::RecvError> for ExtractError {
    fn from(recv: mpsc::error::RecvError) -> Self {
        ExtractError::Recv(recv)
    }
}

impl From<mpsc::error::SendError> for ExtractError {
    fn from(send: mpsc::error::SendError) -> Self {
        ExtractError::Send(send)
    }
}

impl From<str::Utf8Error> for ExtractError {
    fn from(e: str::Utf8Error) -> Self {
        ExtractError::Utf8(e)
    }
}

impl fmt::Display for ExtractError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ExtractError::Redirect(url) => write!(fmt, "redirect to {}", url),
            ExtractError::Recv(e) => write!(fmt, "receiving failure: {}", e),
            ExtractError::Send(e) => write!(fmt, "send failure: {}", e),
            ExtractError::Utf8(e) => write!(fmt, "invalid UTF8: {}", e),
        }
    }
}

fn from_utf8_partial(input: &[u8]) -> Result<(&str, &[u8]), str::Utf8Error> {
    match str::from_utf8(input) {
        Ok(s) => Ok((s, &[])),
        Err(e) => {
            let error_idx = e.valid_up_to();
            if error_idx == 0 {
                return Err(e);
            }
            let (valid, after_valid) = input.split_at(error_idx);
            let s = unsafe { str::from_utf8_unchecked(valid) };
            Ok((s, after_valid))
        }
    }
}

// We extract links by feeding chunks of data from a source channel into the
// `html5ever` tokenizer, which is not `Send`. The reason to use a channel and
// not an `AsyncRead` instance directly is that we want to use the tokio
// threadpool runtime for the actual reading, so we can handle blocking disk
// I/O, but cannot run the tokenizer inside the thread pool.
fn extract(
    base: Arc<Url>,
    source: mpsc::Receiver<Vec<u8>>,
) -> impl Stream<Item = Extracted, Error = ExtractError> {
    let queue = BufferQueue::new();
    let tokenizer = Tokenizer::new(ExtractSink::new(base), TokenizerOpts::default());
    iterate(
        source.map_err(ExtractError::Recv),
        (queue, tokenizer, vec![]),
        |_| (),
        |buf, (mut queue, mut tokenizer, mut remainder)| {
            // FIXME: UTF8 parsing may fail inadvertently at buffer boundaries
            let (s, remainder) = if remainder.len() == 0 {
                from_utf8_partial(&buf)?
            } else {
                remainder.extend(&buf);
                from_utf8_partial(&remainder)?
            };
            queue.push_back(StrTendril::from_slice(s));
            if let TokenizerResult::Script(redirect) = tokenizer.feed(&mut queue) {
                return Err(ExtractError::Redirect(redirect.0));
            }
            Ok((
                tokenizer.sink.output.drain(0..).collect::<Vec<_>>(),
                (queue, tokenizer, Vec::from(remainder)),
            ))
        },
    )
    .map(stream::iter_ok)
    .flatten()
}

struct ExtractSink {
    base: Arc<Url>,
    output: Vec<Extracted>,
}

struct Redirect(Url);

lazy_static! {
    static ref CONTENT_REDIRECT_RE: Regex = Regex::new(r"^(?i)[0-9]+\s*;\s*url=([^;]+)").unwrap();
}

impl ExtractSink {
    fn new(base: Arc<Url>) -> Self {
        ExtractSink {
            base,
            output: vec![],
        }
    }
    fn extract_tag(&mut self, name: &str, attrs: &[Attribute]) -> TokenSinkResult<Redirect> {
        if name == "a" {
            if let Some(href) = attr_value(attrs, "href").and_then(|value| {
                self.base
                    .join(value)
                    .map_err(|_| {
                        eprintln!("could not parse URL `{}'", value);
                    })
                    .ok()
            }) {
                self.output.push(Extracted::Url(href))
            }
        } else if name == "meta" {
            if attrs
                .iter()
                .find(|attr| &attr.name.local == "http-equiv" && attr.value.as_ref() == "refresh")
                .is_some()
            {
                if let Some(content) = attr_value(attrs, "content") {
                    if let Some(url) = CONTENT_REDIRECT_RE
                        .captures(content)
                        .and_then(|c| self.base.join(&c[1]).ok())
                    {
                        return TokenSinkResult::Script(Redirect(url));
                    }
                }
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
            self.output.push(Extracted::Anchor(name.to_string().into()))
        }
        TokenSinkResult::Continue
    }
}

fn attr_value<'a, 'b>(attrs: &'a [Attribute], name: &'b str) -> Option<&'a str> {
    attrs.iter().find_map(|attr| {
        if &attr.name.local == name {
            Some(attr.value.as_ref())
        } else {
            None
        }
    })
}

impl TokenSink for ExtractSink {
    type Handle = Redirect;

    fn process_token(&mut self, token: Token, _line_number: u64) -> TokenSinkResult<Self::Handle> {
        match token {
            // TODO: XML namespace support
            Token::TagToken(Tag {
                ref name,
                ref attrs,
                ..
            }) => self.extract_tag(name.as_ref(), attrs),
            _ => TokenSinkResult::Continue,
        }
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

struct ExtractTask {
    url: Arc<Url>,
    chunk_source: mpsc::Receiver<Vec<u8>>,
}

impl ExtractTask {
    fn run(
        self,
        store: Arc<Store>,
        url_sink: mpsc::Sender<Arc<Url>>,
    ) -> impl Future<Item = (), Error = ExtractError> {
        let url = self.url;
        let store = Arc::clone(&store);
        let resolve_store = Arc::clone(&store);
        let resolve_url = Arc::clone(&url);
        let operations = extract(Arc::clone(&url), self.chunk_source).filter_map(
            move |extracted| match extracted {
                Extracted::Url(link) => {
                    if let Some(unknown_url) = store.add_link(Arc::new(link), Arc::clone(&url)) {
                        Some(Operation::SinkUrl(unknown_url))
                    } else {
                        None
                    }
                }
                Extracted::Anchor(anchor) => Some(Operation::AddAnchor(anchor)),
            },
        );
        iterate(
            operations,
            HashSet::new(),
            move |anchors| {
                resolve_store
                    .resolve(Arc::clone(&resolve_url), anchors)
                    .unwrap_or_else(|e| eprintln!("could not resolve {}: {}", resolve_url, e));
            },
            |op, mut anchors| match op {
                Operation::AddAnchor(anchor) => {
                    anchors.insert(anchor);
                    Ok((None, anchors))
                }
                Operation::SinkUrl(url) => Ok((Some(url), anchors)),
            },
        )
        .filter_map(|item| item)
        .fold(url_sink, move |url_sink, link| url_sink.send(link))
        .map(|_url_sink| ())
    }
}

enum Operation {
    SinkUrl(Arc<Url>),
    AddAnchor(Box<str>),
}

// This is in some ways a mashup between `stream::unfold` and `Stream::fold`. It
// threads a seed through some computation `step`, and when the stream is
// exhausted, it calls `finish` on the final seed value.
fn iterate<S, T, U, F, Fut, It>(
    stream: S,
    init: T,
    finish: U,
    step: F,
) -> impl Stream<Item = It, Error = Fut::Error>
where
    S: Stream<Error = Fut::Error>,
    U: FnOnce(T) -> (),
    F: FnMut(S::Item, T) -> Fut,
    Fut: IntoFuture<Item = (It, T)>,
{
    let transformed = stream::unfold(
        (stream, init, step, finish, false),
        move |(s, seed, mut step, finish, eof)| {
            if eof {
                finish(seed);
                None
            } else {
                let next = s
                    .into_future()
                    .map_err(|(e, _)| e)
                    .and_then(move |(item, rest)| {
                        if let Some(element) = item {
                            future::Either::A(step(element, seed).into_future().map(
                                |(element, seed)| {
                                    (Some(element), (rest, seed, step, finish, false))
                                },
                            ))
                        } else {
                            future::Either::B(future::ok((None, (rest, seed, step, finish, true))))
                        }
                    });
                Some(next)
            }
        },
    )
    .filter_map(|item| item);
    transformed
}

fn extraction_thread(
    store: Arc<Store>,
    tasks: mpsc::Receiver<ExtractTask>,
    url_sink: mpsc::Sender<Arc<Url>>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let extract = tasks.map_err(ExtractError::Recv).for_each(|task| {
            let task_url = Arc::clone(&task.url);
            let store = Arc::clone(&store);
            task.run(Arc::clone(&store), url_sink.clone())
                .or_else(move |e| {
                    match e {
                        ExtractError::Utf8(e) => eprintln!("could not parse {}: {}", task_url, e),
                        ExtractError::Redirect(url) => {
                            store.add_redirect(task_url, Arc::new(url));
                        }
                        _ => return Err(e),
                    }
                    Ok(())
                })
        });
        current_thread::block_on_all(extract).unwrap_or_else(|e| {
            eprintln!("error extracting links: {}", e);
        });
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
                .send(ExtractTask {
                    url: Arc::clone(&url),
                    chunk_source,
                })
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
