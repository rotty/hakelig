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

use failure::{format_err, Error};
use futures::{
    prelude::*,
    stream::{self, BoxStream},
    Stream,
};
use once_cell::sync::Lazy;
use std::io;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{fs, io::AsyncReadExt};
use url::Url;

pub struct Context {
    http_client: reqwest::Client,
}

impl Context {
    pub fn new() -> Self {
        Context {
            http_client: reqwest::Client::new(),
        }
    }
    pub fn http_get(
        &self,
        url: &Url,
    ) -> impl Future<Output = Result<reqwest::Response, reqwest::Error>> {
        self.http_client.get(url.clone()).send()
    }
}

pub type EntityStream = BoxStream<'static, Result<Box<dyn Entity>, Error>>;

type ChunkStream = BoxStream<'static, Result<Vec<u8>, io::Error>>;

pub trait Entity: Send {
    fn url(&self) -> Arc<Url>;
    fn read_chunks(&self, ctx: &Context) -> ChunkStream;
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
    fn read_chunks(&self, _: &Context) -> ChunkStream {
        Box::pin(read_html_file(self.path.clone()).try_flatten_stream())
    }
}

async fn read_html_file(path: Box<Path>) -> Result<ChunkStream, io::Error> {
    let buf = vec![0u8; 4096];
    let file = fs::File::open(path).await?;
    let read_chunks = stream::unfold((buf, file, false), move |(mut buf, mut file, eof)| {
        async move {
            if eof {
                None
            } else {
                let n_read = match file.read(&mut buf).await {
                    Ok(n_read) => n_read,
                    Err(e) => return Some((Err(e), (buf, file, true))),
                };
                if n_read == 0 {
                    Some((Ok(Vec::new()), (buf, file, true)))
                } else {
                    Some((Ok(Vec::from(&buf[..n_read])), (buf, file, false)))
                }
            }
        }
    });
    Ok(Box::pin(read_chunks) as ChunkStream)
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
    fn read_chunks(&self, ctx: &Context) -> ChunkStream {
        // TODO: use dedicated error type
        // TODO: record redirects (use custom RedirectPolicy)
        let chunks = ctx
            .http_get(self.url.as_ref())
            .and_then(|response| {
                async move {
                    let response = response.error_for_status()?;
                    // TODO: would be nice if `reqwest` provided a `chunks`
                    // method returning a stream.
                    Ok(stream::unfold(response, move |mut response| {
                        async move {
                            match response.chunk().await {
                                // TODO: probably should use `Bytes` ourselves to avoid copying here
                                Ok(Some(chunk)) => Some((Ok(Vec::from(&chunk[..])), response)),
                                Ok(None) => None,
                                Err(e) => Some((
                                    Err(io::Error::new(
                                        io::ErrorKind::Other,
                                        format!("reading HTTP response failed: {}", e),
                                    )),
                                    response,
                                )),
                            }
                        }
                    }))
                }
            })
            .map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("HTTP request failed: {}", e))
            })
            .try_flatten_stream();
        Box::pin(chunks)
    }
}

pub fn classify_path(path: &Path) -> Result<Box<dyn Entity>, Error> {
    let ext = match path.extension() {
        Some(ext) => match ext.to_str() {
            Some(ext) => ext,
            None => {
                return Err(format_err!(
                    "path with invalid extension: {}",
                    path.display()
                ))
            }
        },
        None => return Err(format_err!("path without extension: {}", path.display())),
    };
    match ext {
        "html" | "htm" => match HtmlPath::new(&path) {
            Ok(entity) => Ok(Box::new(entity) as Box<dyn Entity>),
            Err(_) => Err(format_err!(
                "could not represent filename {} as an URL",
                path.display()
            )),
        },
        _ => Err(format_err!(
            "path with unknown extension {}: {}",
            ext,
            path.display()
        )),
    }
}

pub fn classify_url(url: &Url) -> Result<Box<dyn Entity>, Error> {
    match url.scheme() {
        "file" => url
            .to_file_path()
            .map_err(|_| format_err!("could not construct file path from URL {}", url))
            .and_then(|path| classify_path(&path)),
        "http" | "https" => Ok(Box::new(HttpUrl::new(Arc::new(url.clone())))),
        _ => Err(format_err!("unsupported URL: {}", url)),
    }
}

type PathStream = BoxStream<'static, Result<Box<Path>, io::Error>>;

fn dir_lister<P>(path: P, predicate: Arc<dyn Fn(&Path) -> bool + Send + Sync>) -> PathStream
where
    P: AsRef<Path> + Send + Sync + 'static,
{
    let entries = fs::read_dir(path)
        .try_flatten_stream()
        .and_then(move |entry| {
            let predicate = predicate.clone();
            async move {
                let path = entry.path();
                let ft = entry.file_type().await?;
                let entries = if ft.is_dir() {
                    dir_lister(path, predicate)
                } else if predicate(&path) {
                    Box::pin(stream::once(async { Ok(path.into_boxed_path()) })) as PathStream
                } else {
                    Box::pin(stream::empty()) as PathStream
                };
                Ok(entries)
            }
        });
    Box::pin(entries.try_flatten())
}

static LOCAL_BASE: Lazy<Url> = Lazy::new(|| Url::parse("file:///").unwrap());

fn entity_stream<S>(stream: S) -> EntityStream
where
    S: Stream<Item = Result<Box<dyn Entity>, Error>> + Send + 'static,
{
    Box::pin(stream)
}

pub fn list_roots<I>(roots: I) -> Result<(Vec<Url>, EntityStream), Error>
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    let mut urls = Vec::new();
    let streams = roots
        .into_iter()
        .map(|root| {
            let root = root.as_ref().to_string();
            match Url::parse(&root) {
                Ok(url) => {
                    urls.push(url.clone());
                    if url.scheme() == "file" {
                        url.to_file_path()
                            .map_err(|_| {
                                format_err!("could not construct file path from URL {}", url)
                            })
                            .map(list_path)
                    } else {
                        classify_url(&url)
                            .map(|entity| entity_stream(stream::once(async { Ok(entity) })))
                    }
                }
                Err(_) => {
                    // FIXME: expect
                    urls.push(LOCAL_BASE.join(&root).expect("cannot parse URL"));
                    Ok(list_path(root.into()))
                }
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((urls, entity_stream(stream::select_all(streams))))
}

fn list_path(path: PathBuf) -> EntityStream {
    let inner_path = path.clone();
    let stream = fs::metadata(path)
        .map_ok(move |metadata| {
            let path = inner_path;
            if metadata.file_type().is_dir() {
                entity_stream(
                    dir_lister(path, Arc::new(|_| true))
                        .err_into()
                        .and_then(|p| async move { classify_path(&p) }),
                )
            } else {
                entity_stream(stream::once(async move { classify_path(path.as_ref()) }))
            }
        })
        .map_err(Error::from)
        .try_flatten_stream();
    Box::pin(stream)
}
