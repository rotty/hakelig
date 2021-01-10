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

use std::{collections::HashSet, fmt, str, sync::Arc, thread};

use futures::{future, Stream, StreamExt};
use html5ever::{
    tokenizer::{
        BufferQueue, Tag, Token, TokenSink, TokenSinkResult, Tokenizer, TokenizerOpts,
        TokenizerResult,
    },
    Attribute,
};
use log::debug;
use once_cell::sync::Lazy;
use regex::Regex;
use tendril::StrTendril;
use tokio::{
    runtime::Runtime,
    sync::{broadcast, mpsc},
    task,
};
use url::Url;

use crate::store::{FoundUrl, Store};

#[derive(Debug)]
enum Extracted {
    Link(Box<str>),
    Anchor(Box<str>),
}

enum ExtractError {
    Redirect(Url),
    Utf8(str::Utf8Error),
}

impl From<str::Utf8Error> for ExtractError {
    fn from(e: str::Utf8Error) -> Self {
        ExtractError::Utf8(e)
    }
}

impl fmt::Display for ExtractError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExtractError::Redirect(url) => write!(fmt, "redirect to {}", url),
            ExtractError::Utf8(e) => write!(fmt, "invalid UTF8: {}", e),
        }
    }
}

struct Redirect(Url);

#[derive(Debug)]
pub struct ExtractTask {
    url: FoundUrl,
    chunk_source: mpsc::Receiver<Vec<u8>>,
}

struct ExtractSink {
    base: Arc<Url>,
    output: Vec<Extracted>,
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

impl ExtractTask {
    pub fn new(url: FoundUrl, chunk_source: mpsc::Receiver<Vec<u8>>) -> Self {
        ExtractTask { url, chunk_source }
    }
    async fn run(
        self,
        url_store: Arc<Store>,
        url_sink: mpsc::UnboundedSender<FoundUrl>,
        mut done_rx: broadcast::Receiver<()>,
        queue_state: super::QueueState,
    ) -> Result<(), ExtractError> {
        queue_state.extraction_dequeued();
        let FoundUrl(url, level) = self.url.clone();
        let extracted_level = level + 1;
        let store_extracted = url_store.recurse_level(extracted_level);
        let store = Arc::clone(&url_store);
        let extracted_stream = extract(Arc::clone(&url), self.chunk_source);
        tokio::pin!(extracted_stream);
        let mut anchors = HashSet::new();
        loop {
            // TODO: Add timeout
            let extracted = tokio::select! {
                extracted = extracted_stream.next() => {
                    match extracted {
                        Some(extracted) => extracted,
                        None => break,
                    }
                }
                _ = done_rx.recv() => break,
            };
            let extracted = match extracted {
                Err(ExtractError::Utf8(e)) => {
                    eprintln!("could not parse {}: {}", self.url, e);
                    continue;
                }
                Err(ExtractError::Redirect(url)) => {
                    store.add_redirect(Arc::clone(&self.url.0), Arc::new(url));
                    continue;
                }
                Ok(extracted) => extracted,
            };
            match extracted {
                Extracted::Link(link) => {
                    if !store_extracted {
                        continue;
                    }
                    if let Some(unknown_url) = store.add_link(Arc::clone(&self.url.0), link) {
                        debug!(
                            "submitting URL {}, extracted from {}",
                            &unknown_url, &self.url
                        );
                        queue_state.url_enqueued();
                        if let Err(e) = url_sink.send(FoundUrl(unknown_url, extracted_level)) {
                            eprintln!("Could not send URL: {}", e);
                            queue_state.url_dequeued();
                        }
                    }
                }
                Extracted::Anchor(anchor) => {
                    anchors.insert(anchor);
                }
            }
        }
        queue_state.extraction_done();
        Ok(())
    }
}

async fn extraction_runner(
    id: u32,
    tasks: async_channel::Receiver<ExtractTask>,
    store: Arc<Store>,
    url_sink: mpsc::UnboundedSender<FoundUrl>,
    done_tx: broadcast::Sender<()>,
    mut done_rx: broadcast::Receiver<()>,
    state: super::QueueState,
) {
    loop {
        let task = tokio::select! {
            task = tasks.recv() => {
                match task {
                    Ok(task) => task,
                    Err(_) => break,
                }
            }
            _ = done_rx.recv() => break,
        };
        debug!("Extraction runner {} got {:?}", id, task);
        if let Err(e) = task
            .run(
                Arc::clone(&store),
                url_sink.clone(),
                done_tx.subscribe(),
                state.clone(),
            )
            .await
        {
            eprintln!("Extraction task failed: {}", e)
        }
        if state.is_done() {
            if let Err(e) = done_tx.send(()) {
                eprintln!("Error notifying completion {}", e);
            }
            break;
        }
        debug!("Extraction runner {} finished a task", id);
    }
    debug!("Extraction runner {} terminated", id);
}

pub fn extraction_thread(
    rt: Arc<Runtime>,
    store: Arc<Store>,
    tasks: async_channel::Receiver<ExtractTask>,
    done_tx: broadcast::Sender<()>,
    url_sink: mpsc::UnboundedSender<FoundUrl>,
    state: super::QueueState,
) -> thread::JoinHandle<anyhow::Result<()>> {
    thread::spawn(move || {
        debug!("Extraction thread running");
        let local = task::LocalSet::new();
        let task_handles = (0..4).map(|i| {
            let extractor = extraction_runner(
                i,
                tasks.clone(),
                Arc::clone(&store),
                url_sink.clone(),
                done_tx.clone(),
                done_tx.subscribe(),
                state.clone(),
            );
            local.spawn_local(extractor)
        });
        debug!("Extraction thread blocking");
        local.block_on(&rt, future::join_all(task_handles));
        debug!("Extraction thread terminating");
        Ok(())
    })
}

// We extract links by feeding chunks of data from a source channel into the
// `html5ever` tokenizer, which is not `Send`. The reason to use a channel and
// not an `AsyncRead` instance directly is that we want to use the tokio
// threadpool runtime for the actual reading, so we can handle blocking disk
// I/O, but cannot run the tokenizer inside the thread pool.
fn extract(
    base: Arc<Url>,
    mut source: mpsc::Receiver<Vec<u8>>,
) -> impl Stream<Item = Result<Extracted, ExtractError>> {
    let mut queue = BufferQueue::new();
    let mut tokenizer = Tokenizer::new(ExtractSink::new(base), TokenizerOpts::default());
    let mut remainder = vec![];
    async_stream::try_stream! {
        while let Some(chunk) = source.recv().await {
            let (s, rest) = if remainder.is_empty() {
                from_utf8_partial(&chunk)?
            } else {
                remainder.extend(&chunk);
                from_utf8_partial(&remainder)?
            };
            queue.push_back(StrTendril::from_slice(s));
            if let TokenizerResult::Script(redirect) = tokenizer.feed(&mut queue) {
                Err(ExtractError::Redirect(redirect.0))?;
            }
            remainder = Vec::from(rest);
            for extracted in tokenizer.sink.output.drain(0..) {
                yield extracted;
            }
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

static CONTENT_REDIRECT_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^(?i)[0-9]+\s*;\s*url=([^;]+)").unwrap());

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

fn attr_value<'a, 'b>(attrs: &'a [Attribute], name: &'b str) -> Option<&'a str> {
    attrs.iter().find_map(|attr| {
        if &attr.name.local == name {
            Some(attr.value.as_ref())
        } else {
            None
        }
    })
}
