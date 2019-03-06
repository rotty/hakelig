use std::collections::{hash_map, HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

use url::Url;

type AnchorSet = HashSet<Box<str>>;

#[derive(Default)]
struct References {
    anchored: HashMap<Box<str>, Vec<Arc<Url>>>,
    plain: Vec<Arc<Url>>,
}

impl References {
    fn add(&mut self, anchor: Option<Box<str>>, referrer: Arc<Url>) {
        if let Some(anchor) = anchor {
            self.add_anchored(anchor, referrer);
        } else {
            self.plain.push(referrer);
        }
    }
    fn add_anchored(&mut self, anchor: Box<str>, referrer: Arc<Url>) {
        self.anchored
            .entry(anchor)
            .or_insert_with(Vec::new)
            .push(referrer);
    }
    fn extend_anchored<I>(&mut self, anchor: Box<str>, referrers: I)
    where
        I: IntoIterator<Item = Arc<Url>>,
    {
        self.anchored
            .entry(anchor)
            .or_insert_with(Vec::new)
            .extend(referrers);
    }
    fn into_inner(self) -> (HashMap<Box<str>, Vec<Arc<Url>>>, Vec<Arc<Url>>) {
        (self.anchored, self.plain)
    }
}

#[derive(Default)]
struct Document {
    anchors: AnchorSet,
    unresolved: References,
}

impl Document {
    fn new(anchors: AnchorSet) -> Self {
        Document {
            anchors,
            unresolved: References::default(),
        }
    }
}

impl Document {
    fn add_referrer(&mut self, anchor: Option<Box<str>>, referrer: Arc<Url>) {
        if let Some(anchor) = anchor {
            if !self.anchors.contains(&anchor) {
                self.unresolved.add_anchored(anchor, referrer);
            }
        }
    }
    fn add_referrers<I>(&mut self, anchor: Option<Box<str>>, referrers: I)
    where
        I: IntoIterator<Item = Arc<Url>>,
    {
        if let Some(anchor) = anchor {
            if !self.anchors.contains(&anchor) {
                self.unresolved.extend_anchored(anchor, referrers);
            }
        }
    }
}

#[derive(Default)]
pub struct Store(Mutex<StoreInner>);

#[derive(Default)]
struct StoreInner {
    // State of these links is unknown, they get moved to `documents` on
    // `resolve`.
    unknown: HashMap<Arc<Url>, References>,
    // Visited documents, indexed by URL.
    documents: HashMap<Arc<Url>, Document>,
}

#[derive(Debug)]
pub enum Error {
    DuplicateDocument,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "duplicate document")
    }
}

impl Store {
    pub fn new() -> Self {
        Store::default()
    }
    pub fn resolve(&self, url: Arc<Url>, anchors: HashSet<Box<str>>) -> Result<(), Error> {
        use hash_map::Entry;
        let mut guard = self.0.lock().expect("store mutex poisoned");
        let references = guard.unknown.remove(&url);
        let doc = match guard.documents.entry(Arc::clone(&url)) {
            Entry::Occupied(_) => return Err(Error::DuplicateDocument),
            Entry::Vacant(vacant) => vacant.insert(Document::new(anchors)),
        };
        if let Some(references) = references {
            let (anchored, _) = references.into_inner();
            for (anchor, referrers) in anchored {
                doc.add_referrers(Some(anchor), referrers);
            }
        }
        Ok(())
    }
    pub fn add_link(&self, url: Arc<Url>, referrer: Arc<Url>) -> Option<Arc<Url>> {
        let (url, fragment) = match url.fragment() {
            Some(fragment) => {
                let mut url = (*url).clone();
                url.set_fragment(None);
                (Arc::new(url), Some(fragment))
            }
            None => (Arc::clone(&url), None),
        };
        let mut guard = self.0.lock().expect("store mutex poisoned");
        if let Some(doc) = guard.documents.get_mut(&url) {
            doc.add_referrer(fragment.map(Into::into), referrer);
            None
        } else {
            guard
                .unknown
                .entry(Arc::clone(&url))
                .or_insert_with(References::default)
                .add(fragment.map(Into::into), referrer);
            Some(url)
        }
    }
    pub fn lock(&self) -> LockedStore {
        LockedStore(self.0.lock().expect("mutex poisoned"))
    }
}

pub struct LockedStore<'a>(MutexGuard<'a, StoreInner>);

impl<'a> LockedStore<'a> {
    pub fn dangling(&'a self) -> impl Iterator<Item = (&'a Url, bool, Vec<&'a Url>)> {
        self.0
            .unknown
            .iter()
            .map(unknown_dangling)
            .chain(self.0.documents.iter().map(document_dangling))
    }
}

fn unknown_dangling<'a>(
    (url, references): (&'a Arc<Url>, &'a References),
) -> (&'a Url, bool, Vec<&'a Url>) {
    let anchored = &references.anchored;
    let plain = &references.plain;
    (
        &url,
        false,
        anchored.values().flatten().map(|r| r.as_ref()).collect(),
    )
}

fn document_dangling<'a>((url, document): (&'a Arc<Url>, &'a Document)) -> (&'a Url, bool, Vec<&'a Url>) {
    unimplemented!()
}
