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

use futures::{channel::mpsc, prelude::*};
use html5ever::tokenizer::{
    BufferQueue, Tag, Token, TokenSink, TokenSinkResult, Tokenizer, TokenizerOpts, TokenizerResult,
};
use html5ever::Attribute;
use log::debug;
use once_cell::sync::Lazy;
use regex::Regex;
use tendril::StrTendril;
use tokio::runtime::current_thread;
use url::Url;

use crate::store::{FoundUrl, Store};

#[derive(Debug)]
enum Extracted {
    Link(Box<str>),
    Anchor(Box<str>),
}

#[derive(Debug)]
enum ExtractError {
    Redirect(Url), // TODO: this is not really an error
    Recv(mpsc::TryRecvError),
    Send(mpsc::SendError),
    Utf8(str::Utf8Error),
}

impl From<mpsc::TryRecvError> for ExtractError {
    fn from(recv: mpsc::TryRecvError) -> Self {
        ExtractError::Recv(recv)
    }
}

impl From<mpsc::SendError> for ExtractError {
    fn from(send: mpsc::SendError) -> Self {
        ExtractError::Send(send)
    }
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

struct Extractor {
    buffer_queue: BufferQueue,
    tokenizer: Tokenizer<ExtractSink>,
    buf: Vec<u8>,
}

impl Extractor {
    fn new(base: Arc<Url>) -> Self {
        Extractor {
            buffer_queue: BufferQueue::new(),
            tokenizer: Tokenizer::new(ExtractSink::new(base), TokenizerOpts::default()),
            buf: Vec::new(),
        }
    }

    fn process_buf(&mut self, buf: Vec<u8>) -> Result<Vec<Extracted>, ExtractError> {
        let (s, remainder) = if self.buf.is_empty() {
            from_utf8_partial(&buf)?
        } else {
            self.buf.extend(buf);
            from_utf8_partial(&self.buf)?
        };
        self.buffer_queue.push_back(StrTendril::from_slice(s));
        if let TokenizerResult::Script(redirect) = self.tokenizer.feed(&mut self.buffer_queue) {
            Err(ExtractError::Redirect(redirect.0))
        } else {
            self.buf = Vec::from(remainder); // TODO: This could be more efficient
            Ok(self.tokenizer.sink.output.drain(0..).collect::<Vec<_>>())
        }
    }
}

struct ExtractSink {
    base: Arc<Url>,
    output: Vec<Extracted>,
}

struct Redirect(Url);

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
    url_sink: mpsc::UnboundedSender<FoundUrl>,
    state: super::QueueState,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut runtime = current_thread::Runtime::new().unwrap();
        let extract = tasks.for_each_concurrent(20, move |task| {
            let store = store.clone();
            let url_sink = url_sink.clone();
            let state = state.clone();
            async move {
                match task.run(store, url_sink, state).await {
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("error running extraction: {}", e);
                    }
                }
            }
        });
        runtime.spawn(extract);
        runtime.run().unwrap_or_else(|e| {
            eprintln!("error extracting links: {}", e);
        });
        debug!("extraction thread terminating");
    })
}

pub struct ExtractTask {
    url: FoundUrl,
    chunk_source: mpsc::Receiver<Vec<u8>>,
}

impl ExtractTask {
    pub fn new(url: FoundUrl, chunk_source: mpsc::Receiver<Vec<u8>>) -> Self {
        ExtractTask { url, chunk_source }
    }
    async fn run(
        mut self,
        store: Arc<Store>,
        mut url_sink: mpsc::UnboundedSender<FoundUrl>,
        state: super::QueueState,
    ) -> Result<(), ExtractError> {
        state.extraction_dequeued();
        let FoundUrl(url, level) = self.url.clone();
        let extracted_level = level + 1;
        let mut extractor = Extractor::new(url.clone());
        let mut anchors = HashSet::new();
        while let Some(chunk) = self.chunk_source.next().await {
            let task_url = Arc::clone(&url);
            match extractor.process_buf(chunk) {
                Ok(items) => {
                    for extracted in items {
                        let task_url = Arc::clone(&task_url);
                        match extracted {
                            Extracted::Link(link) => {
                                if !store.recurse_level(extracted_level) {
                                    continue;
                                }
                                if let Some(unknown_url) =
                                    store.add_link(Arc::clone(&task_url), link)
                                {
                                    debug!(
                                        "submitting URL {}, extracted from {}",
                                        unknown_url, task_url
                                    );
                                    state.url_enqueued();
                                    url_sink
                                        .send(FoundUrl(unknown_url, extracted_level))
                                        .await?;
                                }
                            }
                            Extracted::Anchor(anchor) => {
                                anchors.insert(anchor);
                            }
                        }
                    }
                }
                Err(ExtractError::Utf8(e)) => eprintln!("could not parse {}: {}", task_url, e),
                Err(ExtractError::Redirect(url)) => {
                    store.add_redirect(task_url, Arc::new(url));
                }
                Err(e) => return Err(e), // TODO: return different error type
            }
        }
        if let Err(e) = store.resolve(Arc::clone(&url), anchors) {
            eprintln!("could not resolve {}: {}", url, e);
        }
        debug!("extraction for {} done ({:?})", url, state);
        state.extraction_done();
        if state.is_done() {
            url_sink.close_channel();
        }
        Ok(())
    }
}
