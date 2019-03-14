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

use std::path::{Path, PathBuf};
use std::sync::Arc;

use failure::Error;
use futures::{future, stream, Future, Stream};
use lazy_static::lazy_static;
use reqwest::r#async as rq;
use std::io;
use tokio::fs;
use url::Url;

pub type EntityStream = Box<dyn Stream<Item = Box<dyn Entity>, Error = Error> + Send>;

type ChunkStream = Box<dyn Stream<Item = Vec<u8>, Error = io::Error> + Send>;

pub trait Entity: Send {
    fn url(&self) -> Arc<Url>;
    fn read_chunks(&self) -> ChunkStream;
}

#[derive(Clone)]
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
    fn read_chunks(&self) -> ChunkStream {
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

#[derive(Clone)]
struct HttpUrl {
    url: Arc<Url>,
}

impl HttpUrl {
    fn new(url: Arc<Url>) -> Self {
        HttpUrl { url }
    }
}

impl Entity for HttpUrl {
    fn url(&self) -> Arc<Url> {
        Arc::clone(&self.url)
    }
    fn read_chunks(&self) -> ChunkStream {
        // TODO: use dedicated error type
        // TODO: record redirects (use custom RedirectPolicy)
        let chunks = rq::Client::new()
            .get(self.url.as_ref().clone())
            .send()
            .and_then(|response| response.error_for_status())
            .map(|response| {
                response
                    .into_body()
                    .map(|chunk| Vec::from(chunk.as_ref()))
                    .map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!("reading HTTP response failed: {}", e),
                        )
                    })
            })
            .map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("HTTP request failed: {}", e))
            })
            .flatten_stream();
        Box::new(chunks)
    }
}

pub fn classify_path(path: &Path) -> Option<Box<dyn Entity>> {
    path.extension()
        .and_then(|ext| ext.to_str())
        .and_then(|ext| match ext {
            "html" | "htm" => match HtmlPath::new(&path) {
                Ok(entity) => Some(Box::new(entity) as Box<dyn Entity>),
                Err(_) => {
                    eprintln!("could not represent filename {} as an URL", path.display());
                    None
                }
            },
            _ => None,
        })
}

pub fn classify_url(url: &Url) -> Option<Box<dyn Entity>> {
    match url.scheme() {
        "file" => url
            .to_file_path()
            .map_err(|_| {
                eprintln!("could not construct file path from URL {}", url);
            })
            .ok()
            .and_then(|path| classify_path(&path)),
        "http" | "https" => Some(Box::new(HttpUrl::new(Arc::new(url.clone())))),
        _ => None,
    }
}

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

lazy_static! {
    static ref LOCAL_BASE: Url = Url::parse("file:///").unwrap();
}

fn entity_stream<S>(stream: S) -> EntityStream
where
    S: Stream<Item = Box<dyn Entity>, Error = Error> + Send + 'static,
{
    Box::new(stream)
}

pub fn list_roots<I>(roots: I) -> (Vec<Url>, EntityStream)
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    let mut urls = Vec::new();
    let stream = roots
        .into_iter()
        .fold(entity_stream(stream::empty()), |stream, root| {
            let root = root.as_ref().to_string();
            match Url::parse(&root) {
                Ok(url) => {
                    urls.push(url.clone());
                    if url.scheme() == "file" {
                        match url.to_file_path() {
                            Ok(path) => entity_stream(stream.chain(list_path(path))),
                            Err(_) => stream,
                        }
                    } else {
                        match classify_url(&url) {
                            Some(entity) => entity_stream(stream.chain(stream::once(Ok(entity)))),
                            None => stream,
                        }
                    }
                }
                Err(_) => {
                    urls.push(LOCAL_BASE.join(&root).expect("cannot parse URL"));
                    entity_stream(stream.chain(list_path(root.into())))
                }
            }
        });
    (urls, stream)
}

fn list_path(path: PathBuf) -> EntityStream {
    let inner_path = path.clone();
    let stream = fs::metadata(path)
        .map(move |metadata| {
            let path = inner_path;
            if metadata.file_type().is_dir() {
                entity_stream(
                    dir_lister(path, Arc::new(|_| true))
                        .map_err(Error::from)
                        .filter_map(|p| classify_path(&p)),
                )
            } else {
                entity_stream(stream::once(Ok(path)).filter_map(|p| classify_path(p.as_ref())))
            }
        })
        .map_err(Error::from)
        .flatten_stream();
    Box::new(stream)
}
