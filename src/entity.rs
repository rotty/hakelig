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

use std::{
    collections::VecDeque,
    io,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
};

use anyhow::{anyhow, bail};
use futures::{
    future::{BoxFuture, Either, FutureExt},
    stream::StreamExt,
    Stream,
};
use mime::Mime;
use reqwest::header;
use tokio::{fs, io::AsyncReadExt};
use url::Url;

use crate::store::FoundUrl;

pub struct RootList {
    roots: VecDeque<Url>,
    state: Option<DirWalk>,
}

impl RootList {
    pub async fn next(&mut self) -> anyhow::Result<Option<Url>> {
        loop {
            match &mut self.state {
                Some(walk) => match walk.next().await? {
                    Some(path) => {
                        let url =
                            Url::from_file_path(path).expect("Dir walker returned relative path");
                        return Ok(Some(url));
                    }
                    None => self.state = None,
                },
                None => {
                    let root = match self.roots.pop_front() {
                        Some(root) => root,
                        None => return Ok(None),
                    };
                    if root.scheme() == "file" {
                        let path = root.to_file_path().expect("Non-local file URL");
                        let metadata = match fs::symlink_metadata(&path).await {
                            Ok(metadata) => metadata,
                            Err(e) => bail!(
                                "Could not obtain metadata for path {}: {}",
                                path.display(),
                                e
                            ),
                        };
                        if metadata.is_dir() {
                            let mut walk = DirWalk::new(path).await?;
                            if let Some(item) = walk.next().await? {
                                self.state = Some(walk);
                                let url = Url::from_file_path(item)
                                    .expect("Dir walker returned relative path");
                                return Ok(Some(url));
                            }
                        } else {
                            return Ok(Some(root));
                        }
                    } else {
                        return Ok(Some(root));
                    }
                }
            }
        }
    }
}

struct DirWalk {
    stack: Vec<fs::ReadDir>,
}

impl DirWalk {
    async fn new<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let reader = fs::read_dir(path.as_ref()).await?;
        Ok(Self {
            stack: vec![reader],
        })
    }

    async fn next(&mut self) -> anyhow::Result<Option<PathBuf>> {
        loop {
            let top = match self.stack.last_mut() {
                Some(top) => top,
                None => return Ok(None),
            };
            match top.next_entry().await? {
                Some(entry) => {
                    let ft = entry.file_type().await?;
                    if ft.is_dir() {
                        let dir_cursor = fs::read_dir(entry.path()).await?;
                        self.stack.push(dir_cursor);
                    } else {
                        return Ok(Some(entry.path()));
                    }
                }
                None => {
                    let _ = self.stack.pop().unwrap();
                }
            }
        }
    }
}

pub fn list_roots(roots: &[Url]) -> RootList {
    RootList {
        roots: VecDeque::from(roots.to_vec()),
        state: None,
    }
}

pub struct Context {
    http_client: reqwest::Client,
}

impl Context {
    pub fn new() -> Self {
        Context {
            http_client: reqwest::Client::new(),
        }
    }
    pub async fn http_get(&self, url: &Url) -> reqwest::Result<reqwest::Response> {
        Ok(self
            .http_client
            .get(url.clone())
            .send()
            .await?
            .error_for_status()?)
    }
}

type ChunkStream = Pin<Box<dyn Stream<Item = anyhow::Result<Vec<u8>>> + Send>>;
pub type ReadFuture = BoxFuture<'static, anyhow::Result<(Mime, ChunkStream)>>;

pub trait Entity: Send + Unpin {
    fn found_url(&self) -> &FoundUrl;

    fn url(&self) -> Arc<Url> {
        self.found_url().0.clone()
    }

    fn level(&self) -> u32 {
        self.found_url().1
    }

    fn read(&self, ctx: Arc<Context>) -> ReadFuture;
}

#[derive(Clone)]
struct HttpUrl {
    url: FoundUrl,
}

impl HttpUrl {
    fn new(url: FoundUrl) -> Self {
        HttpUrl { url }
    }
}

impl Entity for HttpUrl {
    fn found_url(&self) -> &FoundUrl {
        &self.url
    }
    fn read(&self, ctx: Arc<Context>) -> ReadFuture {
        // TODO: use dedicated error type
        // TODO: record redirects (use custom RedirectPolicy)
        let url = self.url();
        async move {
            let mut response = ctx.http_get(&url).await?;
            let mime = determine_mime_type(&response, &url)?;
            let chunks = async_stream::try_stream! {
                while let Some(chunk) = response.chunk().await? {
                    yield Vec::from(chunk.as_ref());
                }
            };
            Ok((mime, chunks.boxed()))
        }
        .boxed()
    }
}

fn determine_mime_type(response: &reqwest::Response, url: &Url) -> anyhow::Result<Mime> {
    if let Some(content_type) = response.headers().get(header::CONTENT_TYPE) {
        Ok(content_type.to_str()?.parse()?)
    } else if let Some(name) = url
        .path_segments()
        .and_then(|mut segments| segments.next_back())
    {
        // TODO: consolidate this logic with `classify_path`
        let ext = name.split('.').next_back().ok_or_else(|| {
            anyhow!("non content header, and no file extension, cannot determine MIME type")
        })?;
        match ext {
            "html" | "htm" => Ok(mime::TEXT_HTML),
            _ => bail!("unsupported MIME type {}", name),
        }
    } else {
        bail!("unable to detect MIME type");
    }
}

struct HtmlPath {
    path: Box<Path>,
    url: FoundUrl,
}

impl HtmlPath {
    fn new<P: AsRef<Path>>(path: P, level: u32) -> io::Result<Self> {
        let mut abs_path = std::env::current_dir()?;
        abs_path.push(path);
        let url = Url::from_file_path(&abs_path).expect("unrepresentable path");
        Ok(HtmlPath {
            path: abs_path.into(),
            url: FoundUrl(Arc::new(url), level),
        })
    }
}

impl Entity for HtmlPath {
    fn found_url(&self) -> &FoundUrl {
        &self.url
    }
    fn read(&self, _: Arc<Context>) -> ReadFuture {
        let mut buf = vec![0u8; 4096];
        let path = self.path.clone();
        async move {
            let file = fs::File::open(path).await;
            let chunks = match file {
                Ok(mut file) => Either::Right(async_stream::try_stream! {
                    loop {
                        let n_read = file.read(&mut buf).await?;
                        if n_read == 0 {
                            break;
                        } else {
                            yield Vec::from(&buf[..n_read]);
                        }
                    }
                }),
                Err(e) => Either::Left(tokio_stream::once(Err(e.into()))),
            };
            Ok((mime::TEXT_HTML, chunks.boxed()))
        }
        .boxed()
    }
}

fn classify_path(path: &Path, level: u32) -> anyhow::Result<Box<dyn Entity>> {
    let ext = match path.extension() {
        Some(ext) => match ext.to_str() {
            Some(ext) => ext,
            None => return Err(anyhow!("path with invalid extension: {}", path.display())),
        },
        None => return Err(anyhow!("path without extension: {}", path.display())),
    };
    match ext {
        "html" | "htm" => match HtmlPath::new(&path, level) {
            Ok(entity) => Ok(Box::new(entity) as Box<dyn Entity>),
            Err(_) => Err(anyhow!(
                "could not represent filename {} as an URL",
                path.display()
            )),
        },
        _ => Err(anyhow!(
            "path with unknown extension {}: {}",
            ext,
            path.display()
        )),
    }
}

pub fn classify_url(url: FoundUrl) -> anyhow::Result<Box<dyn Entity>> {
    match url.0.scheme() {
        "file" => url
            .0
            .to_file_path()
            .map_err(|_| anyhow!("could not construct file path from URL {}", url))
            .and_then(|path| classify_path(&path, url.1)),
        "http" | "https" => Ok(Box::new(HttpUrl::new(url))),
        _ => bail!("Unsupported URL: {}", url),
    }
}
