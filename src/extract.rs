use std::collections::HashSet;
use std::fmt;
use std::str;
use std::sync::Arc;
use std::thread;

use futures::{future, stream, Future, IntoFuture, Stream};
use html5ever::tokenizer::{
    BufferQueue, Tag, Token, TokenSink, TokenSinkResult, Tokenizer, TokenizerOpts, TokenizerResult,
};
use html5ever::Attribute;
use lazy_static::lazy_static;
use log::debug;
use regex::Regex;
use tendril::StrTendril;
use tokio::runtime::current_thread;
use tokio::sync::mpsc;
use url::Url;

use crate::store::Store;

#[derive(Debug)]
enum Extracted {
    Link(Box<str>),
    Anchor(Box<str>),
}

enum ExtractError {
    Finished,
    Redirect(Url),
    UnboundedSend(mpsc::error::UnboundedSendError),
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

impl From<mpsc::error::UnboundedSendError> for ExtractError {
    fn from(send: mpsc::error::UnboundedSendError) -> Self {
        ExtractError::UnboundedSend(send)
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
            ExtractError::Finished => write!(fmt, "finished"),
            ExtractError::Redirect(url) => write!(fmt, "redirect to {}", url),
            ExtractError::Recv(e) => write!(fmt, "receiving failure: {}", e),
            ExtractError::Send(e) => write!(fmt, "send failure: {}", e),
            ExtractError::UnboundedSend(e) => write!(fmt, "unbounded send failure: {}", e),
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
            let (s, remainder) = if remainder.is_empty() {
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

/// Checks for a `http-equiv=refresh` attribute and returns the URL inside the
/// `content` attribute, if present.
fn meta_refresh_url(attrs: &[Attribute]) -> Option<&str> {
    attrs
        .iter()
        .find(|attr| &attr.name.local == "http-equiv" && attr.value.as_ref() == "refresh")?;
    attr_value(attrs, "content").and_then(|content| {
        CONTENT_REDIRECT_RE
            .captures(content)
            .map(|c| c.get(1).unwrap().as_str())
    })
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
            if let Some(href) = attr_value(attrs, "href") {
                self.output.push(Extracted::Link(href.into()))
            }
        } else if name == "meta" {
            if let Some(url) = meta_refresh_url(attrs).and_then(|url| self.base.join(url).ok()) {
                return TokenSinkResult::Script(Redirect(url));
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

pub fn extraction_thread(
    store: Arc<Store>,
    tasks: mpsc::Receiver<ExtractTask>,
    url_sink: mpsc::UnboundedSender<Arc<Url>>,
    state: super::QueueState,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let extract = tasks
            .map_err(ExtractError::Recv)
            .map(move |task| task.run(Arc::clone(&store), url_sink.clone(), state.clone()))
            .buffer_unordered(10)
            .for_each(|_| Ok(()));
        current_thread::block_on_all(extract).unwrap_or_else(|e| {
            eprintln!("error extracting links: {}", e);
        });
        debug!("extraction thread terminating");
    })
}

pub struct ExtractTask(TaskInner);

enum TaskInner {
    Sentinel,
    Process {
        url: Arc<Url>,
        chunk_source: mpsc::Receiver<Vec<u8>>,
    },
}

impl ExtractTask {
    pub fn new(url: Arc<Url>, chunk_source: mpsc::Receiver<Vec<u8>>) -> Self {
        ExtractTask(TaskInner::Process { url, chunk_source })
    }
    pub fn sentinel() -> Self {
        ExtractTask(TaskInner::Sentinel)
    }
    fn run(
        self,
        store: Arc<Store>,
        url_sink: mpsc::UnboundedSender<Arc<Url>>,
        state: super::QueueState,
    ) -> impl Future<Item = (), Error = ExtractError> {
        match self.0 {
            TaskInner::Sentinel => {
                let process_sentinel = future::lazy(move || {
                    if state.is_done() {
                        return Err(ExtractError::Finished);
                    }
                    Ok(())
                });
                future::Either::A(process_sentinel)
            }
            TaskInner::Process { url, chunk_source } => {
                future::Either::B(process_url(url, chunk_source, store, url_sink, state))
            }
        }
        //dbg!(&self.url);
    }
}

fn process_url(
    task_url: Arc<Url>,
    chunk_source: mpsc::Receiver<Vec<u8>>,
    url_store: Arc<Store>,
    url_sink: mpsc::UnboundedSender<Arc<Url>>,
    queue_state: super::QueueState,
) -> impl Future<Item = (), Error = ExtractError> {
    queue_state.extraction_dequeued();
    let store = Arc::clone(&url_store);
    let url = Arc::clone(&task_url);
    let operations = extract(Arc::clone(&url), chunk_source).filter_map(move |extracted| {
        //dbg!(&extracted);
        match extracted {
            Extracted::Link(link) => {
                if let Some(unknown_url) = store.add_link(Arc::clone(&url), link) {
                    Some(Operation::SinkUrl(unknown_url))
                } else {
                    None
                }
            }
            Extracted::Anchor(anchor) => Some(Operation::AddAnchor(anchor)),
        }
    });
    let url = Arc::clone(&task_url);
    let store = Arc::clone(&url_store);
    let found_urls = iterate(
        operations,
        HashSet::new(),
        move |anchors| {
            store
                .resolve(Arc::clone(&url), anchors)
                .unwrap_or_else(|e| eprintln!("could not resolve {}: {}", url, e));
        },
        |op, mut anchors| {
            //dbg!(&op);
            match op {
                Operation::AddAnchor(anchor) => {
                    anchors.insert(anchor);
                    Ok((None, anchors))
                }
                Operation::SinkUrl(url) => Ok((Some(url), anchors)),
            }
        },
    )
    .filter_map(|item| item);

    let url = Arc::clone(&task_url);
    let state = queue_state.clone();
    let found_debug = found_urls.map(move |item| {
        debug!("submitting URL {}, extracted from {}", &item, &url);
        state.url_enqueued(); // TODO: is this really _sure_?
        item
    });
    let url = Arc::clone(&task_url);
    let state = queue_state;
    let store = Arc::clone(&url_store);
    found_debug
        .forward(url_sink)
        .map(move |_url_sink| ())
        .then(move |result| {
            debug!("extraction for {} done ({:?})", url, state);
            state.extraction_done();
            if state.is_done() {
                return Err(ExtractError::Finished);
            }
            result
        })
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
}

#[derive(Debug)]
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
    stream::unfold(
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
    .filter_map(|item| item)
}
