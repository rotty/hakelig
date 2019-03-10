# hakelig: A fast link checker written in Rust

This project is currently in the proof-of-concept stage, the codebase
and its output is quite rough, so it is not yet intended for actual
use, but for interested developers.

The first milestone will be to make it useable for quickly checking
documentation emitted by `cargo doc` to the local filesystem.

## What works

- Local, recursive link checking for HTML documents, using the
  [`html5ever`][html5ever] tokenizer to extract the links.

- Support for URL fragments (anchors).

- Concurrent, futures-based architecture using [`tokio`][tokio]. The
  retrieval side can operate on many documents in parallel, making use
  of Tokio's threadpool. The extraction is done concurrently as well,
  but in a single thread, as `html5ever` is not thread-safe. So if you
  can ingest more data than a single core can extract URLs from, this
  will be a bottleneck. Otherwise `hakelig` aims to be as scalable as
  possible.

  Although the code has not been optimized at the slightest, `hakelig`
  can churn through about 27MiB of link-rich documentation output by
  `cargo doc` in about a second.

[tokio]: https://tokio.rs/
[html5ever]: https://github.com/servo/html5ever

## What's next

- [ ] Reasonable output and progress indication, probably taking
      [linkchecker] as a source of inspiration.

- [ ] HTTP support.

[linkchecker]: https://linkchecker.github.io/linkchecker/

## License

The code and documentation in the `hakelig` crate is [free software],
licensed under the [GNU GPL](./LICENSE) version 2, or (at your option)
any later version of the GNU GPL.

[free software]: https://www.gnu.org/philosophy/free-sw.html
