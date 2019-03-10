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

use std::path::Path;
use std::sync::Arc;

use failure::Error;
use futures::{stream, Future, Stream};
use reqwest::r#async as rq;
use std::io;
use tokio::fs;
use url::Url;

pub type EntityStream = Box<dyn Stream<Item = Box<dyn Entity + Send>, Error = Error> + Send>;

type ChunkStream = Box<dyn Stream<Item = Vec<u8>, Error = io::Error> + Send>;

pub trait Entity {
    fn url(&self) -> Arc<Url>;
    fn read_chunks(&self) -> ChunkStream;
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

pub fn classify_path(path: &Path) -> Option<Box<dyn Entity + Send>> {
    path.extension()
        .and_then(|ext| ext.to_str())
        .and_then(|ext| match ext {
            "html" | "htm" => match HtmlPath::new(&path) {
                Ok(entity) => Some(Box::new(entity) as Box<Entity + Send>),
                Err(_) => {
                    eprintln!("could not represent filename {} as an URL", path.display());
                    None
                }
            },
            _ => None,
        })
}

pub fn classify_url(url: &Url) -> Option<Box<dyn Entity + Send>> {
    match url.scheme() {
        "file" => url
            .to_file_path()
            .map_err(|_| {
                eprintln!("could not construct file path from URL {}", url);
            })
            .ok()
            .and_then(|path| classify_path(&path)),
        "http" => Some(Box::new(HttpUrl::new(Arc::new(url.clone())))),
        _ => None,
    }
}
