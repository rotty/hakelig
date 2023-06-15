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

//! A thread-safe store for the link-checking results.

#![allow(dead_code)]

use std::collections::{hash_map, HashMap, HashSet};
use std::fmt;
use std::iter;
use std::sync::{Arc, Mutex, MutexGuard};

use regex::RegexSet;
use url::Url;

#[derive(Debug, Clone)]
pub struct FoundUrl(pub Arc<Url>, pub u32);

impl FoundUrl {
    pub fn new(url: Url) -> Self {
        FoundUrl(Arc::new(url), 0)
    }
}

impl fmt::Display for FoundUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<{}>[{}]", self.0, self.1)
    }
}

type AnchorSet = HashSet<Box<str>>;

/// The references to a document.
#[derive(Default, Debug)]
struct References {
    /// For each anchor, the list of referrers
    anchored: HashMap<Box<str>, Vec<Referrer>>,
    /// The list of referrers that do not have a fragment (i.e. reference the
    /// document as a whole).
    plain: Vec<Referrer>,
}

impl References {
    fn is_empty(&self) -> bool {
        self.anchored.is_empty() && self.plain.is_empty()
    }
    fn add(&mut self, anchor: Option<Box<str>>, referrer: Referrer) {
        if let Some(anchor) = anchor {
            self.add_anchored(anchor, referrer);
        } else {
            self.plain.push(referrer);
        }
    }
    fn add_anchored(&mut self, anchor: Box<str>, referrer: Referrer) {
        self.anchored
            .entry(anchor)
            .or_insert_with(Vec::new)
            .push(referrer);
    }
    fn extend_anchored<I>(&mut self, anchor: Box<str>, referrers: I)
    where
        I: IntoIterator<Item = Referrer>,
    {
        self.anchored
            .entry(anchor)
            .or_insert_with(Vec::new)
            .extend(referrers);
    }
    fn into_inner(self) -> (HashMap<Box<str>, Vec<Referrer>>, Vec<Referrer>) {
        (self.anchored, self.plain)
    }
    fn referrers(&self) -> impl Iterator<Item = &Referrer> {
        // TODO: uniquify referrers
        self.anchored.values().flatten().chain(self.plain.iter())
    }
    fn anchored(&self) -> impl Iterator<Item = (&str, impl Iterator<Item = &Referrer>)> {
        self.anchored
            .iter()
            .map(|(a, referrers)| (a.as_ref(), referrers.iter()))
    }
    fn plain(&self) -> impl Iterator<Item = &Referrer> {
        self.plain.iter()
    }
}

/// A reference to some other URL.
#[derive(Debug)]
pub struct Referrer {
    /// The referencing URL.
    url: Arc<Url>,
    /// The link (href attribute) in that URL.
    href: Box<str>,
}

impl Referrer {
    pub fn url(&self) -> &Url {
        &self.url
    }
    pub fn href(&self) -> &str {
        &self.href
    }
}

#[derive(Debug)]
pub enum Error {
    DuplicateDocument,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "duplicate document")
    }
}

struct Document {
    resolved: Resolved,
    unresolved: References,
}

enum Resolved {
    Success { anchors: AnchorSet },
    Failure { msg: String },
}

impl Document {
    fn new(anchors: AnchorSet) -> Self {
        Document {
            resolved: Resolved::Success { anchors },
            unresolved: References::default(),
        }
    }
    fn error(msg: String) -> Self {
        Document {
            resolved: Resolved::Failure { msg },
            unresolved: References::default(),
        }
    }
    fn has_anchor(&self, anchor: &str) -> bool {
        match &self.resolved {
            Resolved::Failure { .. } => false,
            Resolved::Success { anchors } => anchors.contains(anchor),
        }
    }
    fn add_referrer(&mut self, anchor: Option<Box<str>>, referrer: Referrer) {
        if let Some(anchor) = anchor {
            if !self.has_anchor(&anchor) {
                self.unresolved.add_anchored(anchor, referrer);
            }
        }
    }
    fn add_referrers<I>(&mut self, anchor: Option<Box<str>>, referrers: I)
    where
        I: IntoIterator<Item = Referrer>,
    {
        if let Some(anchor) = anchor {
            if !self.has_anchor(&anchor) {
                self.unresolved.extend_anchored(anchor, referrers);
            }
        }
    }
}

pub struct Store {
    // Links are ignored (not followed) if they match this pattern.
    link_ignore: RegexSet,
    // Set of root urls, every URL below these is considered internal, and
    // queued for link extraction. If `None`, recursion is not restricted by
    // URL.
    restrict_urls: Option<Vec<Url>>,
    // Maximum recursion level (inclusive).
    recursion_level: Option<u32>,
    // The mutable part.
    inner: Mutex<StoreInner>,
}

#[derive(Default)]
struct StoreInner {
    // State of these links is unknown, they get moved to `documents` on
    // `resolve`.
    unknown: HashMap<Arc<Url>, References>,
    // Visited documents, indexed by URL.
    documents: HashMap<Arc<Url>, Document>,
    // Redirected URLs
    redirects: HashMap<Arc<Url>, Arc<Url>>,
    // URLs we have already touched (i.e. are about to process or already have processed)
    touched: HashSet<Arc<Url>>,
}

impl Store {
    fn is_restricted(&self, url: &Url) -> bool {
        if let Some(restrictions) = &self.restrict_urls {
            !restrictions
                .iter()
                .any(|restrict_url| url_is_below(restrict_url, url))
        } else {
            false
        }
    }
}

fn is_path_prefix<'a, T, U>(prefix: T, seq: U) -> bool
where
    T: IntoIterator<Item = &'a str>,
    U: IntoIterator<Item = &'a str>,
{
    let mut seq = seq.into_iter();
    for elt in prefix.into_iter() {
        if elt.is_empty() {
            continue;
        }
        match seq.next() {
            None => return false,
            Some(seq_elt) => {
                if elt != seq_elt {
                    return false;
                }
            }
        }
    }
    true
}

fn url_is_below(base: &Url, url: &Url) -> bool {
    url.scheme() == base.scheme()
        && url.host_str() == base.host_str()
        && url.port_or_known_default() == base.port_or_known_default()
        && match (base.path_segments(), url.path_segments()) {
            (None, None) => true,
            (Some(base_segments), Some(url_segments)) => {
                is_path_prefix(base_segments, url_segments)
            }
            _ => false,
        }
}

impl Store {
    pub fn new(
        link_ignore: RegexSet,
        restrict: Option<Vec<Url>>,
        recursion_level: Option<u32>,
    ) -> Self {
        Store {
            link_ignore,
            restrict_urls: restrict,
            inner: Mutex::new(StoreInner::default()),
            recursion_level,
        }
    }
    pub fn recurse_level(&self, level: u32) -> bool {
        if let Some(max_level) = self.recursion_level {
            level <= max_level
        } else {
            true
        }
    }
    fn resolve_document<F>(&self, url: Arc<Url>, f: F) -> Result<(), Error>
    where
        F: FnOnce() -> Document,
    {
        use hash_map::Entry;
        let mut guard = self.inner.lock().expect("store mutex poisoned");
        let references = guard.unknown.remove(&url);
        let doc = match guard.documents.entry(Arc::clone(&url)) {
            Entry::Occupied(_) => return Err(Error::DuplicateDocument),
            Entry::Vacant(vacant) => vacant.insert(f()),
        };
        if let Some(references) = references {
            let (anchored, _) = references.into_inner();
            for (anchor, referrers) in anchored {
                doc.add_referrers(Some(anchor), referrers);
            }
        }
        Ok(())
    }
    pub fn resolve(&self, url: Arc<Url>, anchors: HashSet<Box<str>>) -> Result<(), Error> {
        self.resolve_document(url, move || Document::new(anchors))
    }
    pub fn resolve_error(&self, url: Arc<Url>, msg: String) -> Result<(), Error> {
        self.resolve_document(url, move || Document::error(msg))
    }
    pub fn touch(&self, url: Arc<Url>) -> bool {
        let mut guard = self.inner.lock().expect("store mutex poisoned");
        guard.touched.insert(url)
    }

    pub fn add_link(&self, document_url: Arc<Url>, href: Box<str>) -> Option<Arc<Url>> {
        let target = document_url
            .join(&href)
            .map_err(|_| {
                // FIXME: this should not output
                eprintln!("could not parse link `{}'", href);
            })
            .ok()?;
        if self.link_ignore.is_match(target.as_str()) || self.is_restricted(&target) {
            return None;
        }
        let mut guard = self.inner.lock().expect("store mutex poisoned");
        let (url, fragment) = match target.fragment() {
            Some(fragment) if !fragment.is_empty() => {
                let mut target = target.clone();
                target.set_fragment(None);
                (Arc::new(target), Some(fragment))
            }
            _ => (Arc::new(target), None),
        };
        let referrer = Referrer {
            url: document_url,
            href,
        };
        let url = Arc::clone(guard.redirects.get(&url).unwrap_or(&url));
        if let Some(doc) = guard.documents.get_mut(&url) {
            doc.add_referrer(fragment.map(Into::into), referrer);
            None
        } else if let Some(unknown) = guard.unknown.get_mut(&url) {
            unknown.add(fragment.map(Into::into), referrer);
            None
        } else {
            guard
                .unknown
                .entry(Arc::clone(&url))
                .or_insert_with(References::default)
                .add(fragment.map(Into::into), referrer);
            if guard.touched.get(&url).is_some() {
                None
            } else {
                Some(url)
            }
        }
    }
    pub fn add_redirect(&self, url: Arc<Url>, to: Arc<Url>) {
        let mut guard = self.inner.lock().expect("store mutex poisoned");
        if let Some(references) = guard.unknown.remove(&url) {
            guard.unknown.insert(Arc::clone(&to), references);
        }
        guard.redirects.insert(url, to);
    }
    pub fn lock(&self) -> LockedStore<'_> {
        LockedStore(self.inner.lock().expect("mutex poisoned"))
    }
}

pub struct LockedStore<'a>(MutexGuard<'a, StoreInner>);

impl<'a> LockedStore<'a> {
    pub fn dangling(&'a self) -> impl Iterator<Item = (&'a Url, bool, Vec<&'a Referrer>)> {
        self.0
            .unknown
            .iter()
            .map(unknown_dangling)
            .chain(self.0.documents.iter().filter_map(document_dangling))
    }
    pub fn known_dangling(
        &'a self,
    ) -> impl Iterator<
        Item = (
            &'a Url,
            impl Iterator<Item = (Option<&'a str>, Vec<&'a Referrer>)>,
        ),
    > {
        self.0.documents.iter().filter_map(document_known_dangling)
    }
}

fn unknown_dangling<'a>(
    (url, references): (&'a Arc<Url>, &'a References),
) -> (&'a Url, bool, Vec<&'a Referrer>) {
    (url, false, references.referrers().collect())
}

fn document_dangling<'a>(
    (url, document): (&'a Arc<Url>, &'a Document),
) -> Option<(&'a Url, bool, Vec<&'a Referrer>)> {
    if document.unresolved.is_empty() {
        None
    } else {
        Some((url, true, document.unresolved.referrers().collect()))
    }
}

fn document_known_dangling<'a>(
    (url, document): (&'a Arc<Url>, &'a Document),
) -> Option<(
    &'a Url,
    impl Iterator<Item = (Option<&'a str>, Vec<&'a Referrer>)>,
)> {
    if document.unresolved.is_empty() {
        None
    } else {
        let none: Option<&'a str> = None;
        let plain = iter::once((none, document.unresolved.plain().collect()));
        let anchored = document
            .unresolved
            .anchored()
            .map(|(anchor, referrers)| (Some(anchor), referrers.collect()));
        Some((url, plain.chain(anchored)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_prefix() {
        assert!(is_path_prefix(vec![], vec![]));
        assert!(is_path_prefix(vec![], vec!["a", "b", "c"]));
        assert!(is_path_prefix(vec!["a", "b"], vec!["a", "b", "c"]));
        assert!(!is_path_prefix(vec!["a", "b", "c"], vec!["a", "b"]));
    }

    #[test]
    fn test_url_is_below() {
        assert!(url_is_below(
            &Url::parse("http://example.com/").unwrap(),
            &Url::parse("http://example.com/foo").unwrap(),
        ));
        assert!(url_is_below(
            &Url::parse("http://example.com/").unwrap(),
            &Url::parse("http://example.com/").unwrap(),
        ));
    }
}
