#![feature(impl_trait_in_assoc_type)]

//! Abbreviations:
//!
//! - attr ~ attribute
//! - cs ~ content server
//! - req ~ request
//! - resp ~ response
//! - rh ~ range header
//!
//! They are used in other crates too.

pub mod range_from;
pub mod byte_content_range;

use std::{future::Future, task::Poll, ops::{Range, RangeInclusive}};
use pin_project::pin_project;
use std::fmt::Debug;
use std::collections::VecDeque;
use bytes::Bytes;
use base64::engine::{Engine as _, general_purpose::URL_SAFE};
use tracing::{error, warn, info, debug};
use tokio::sync::{mpsc, oneshot};
use hyper::{server::conn::http1, service::service_fn};
use http::{
    request::{self, Request},
    response::{self, Response},
    status::StatusCode,
};
use vincenzo::disk::{
    slice::{INFO_HASH_BIT_N, InfoHash, SliceId},
    SliceError,
    FileId,
    FileByPathR,
    DiskMsg,
};
use range_from::{TryFromRange as _, TryFromRangeError};
use byte_content_range::ContentRange;



#[derive(Debug, thiserror::Error)]
enum ParseRhError {
    #[error("the Range HTTP header value contains
        an invisible ASCII character: {0}")]
    ToStr(http::header::ToStrError),

    #[error("syntactically invalid Range HTTP header value: {0}")]
    RangeUnsatisfiable(http_range_header::RangeUnsatisfiableError),
}

/// Parses a Range HTTP header in `headers`.
/// If `headers` does not contain such a header, returns `None`.
/// Otherwise, returns `Some(_)`.
fn parse_rh(
    headers: &http::HeaderMap<http::HeaderValue>
) -> Option<Result<http_range_header::ParsedRanges, ParseRhError>> {
    headers.get(http::header::RANGE).map(|range|
        match range.to_str() {
            Err(e) => Err(ParseRhError::ToStr(e)),
            Ok(range) => match http_range_header::parse_range_header(range) {
                Err(e) => Err(ParseRhError::RangeUnsatisfiable(e)),
                Ok(range) => Ok(range),
            },
        }
    )
}

#[derive(Debug, thiserror::Error)]
enum RhInFileError {
    #[error("only one file range is supported")]
    RangeNotOne,

    #[error("file ranges are overlapping or out of bounds: {0}")]
    RangeUnsatisfiable(http_range_header::RangeUnsatisfiableError),

    #[error("internal error: file range conversion: {0}")]
    RangeConvert(TryFromRangeError),
}

/// Returns a range with specific bounds that represents the same set
/// as `rh` for a file of size `file_size`.
fn rh_in_file(
    file_size: u64,
    rh: http_range_header::ParsedRanges,
) -> Result<Range<u64>, RhInFileError> {
    match rh.validate(file_size) {
        Err(e) => Err(RhInFileError::RangeUnsatisfiable(e)),
        Ok(mut ranges) => if ranges.len() != 1 {
            Err(RhInFileError::RangeNotOne)
        } else {
            Range::try_from_range(ranges.pop().unwrap())
                .map_err(RhInFileError::RangeConvert)
        },
    }
}



fn decode_info_hash(info_hash: &str) -> Result<InfoHash, ()> {
    let info_hash = info_hash.as_bytes();

    /// The number of characters in a Base64 encoding of a SHA-1 hash.
    const INFO_HASH_BASE64_CHAR_N: u8 = INFO_HASH_BIT_N
        .div_ceil(6)
        .div_ceil(4)
        * 4;

    if info_hash.len() != INFO_HASH_BASE64_CHAR_N.into() { Err(()) } else {
        match URL_SAFE.decode(info_hash) {
            Err(_) => Err(()),
            Ok(info_hash) => info_hash.try_into().map_err(|_| ()),
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum ParseUriPathError {
    #[error("splitting the URI path yielded zero elements")]
    Split,

    #[error("the URI path should start with '/'")]
    StartSlash,

    #[error("the URI path should start with an info hash")]
    NoInfoHash,

    #[error("invalid info hash in the URI path")]
    InvalidInfoHash,

    #[error("a URI path component is not encoded with UTF-8")]
    FromUtf8,
}

/// Web routing.
/// Validates and converts a URI path to a file in a torrent.
/// If it returns `Ok((info_hash, path))`, then `uri_path` refers
/// to the file in the torrent identified by `info_hash`
/// at `path` relative to the root of the torrent.
fn parse_uri_path(
    uri_path: &str,
) -> Result<(InfoHash, Vec<String>), ParseUriPathError> {
    let mut uri_path = uri_path.split('/').collect::<VecDeque<_>>();
    match uri_path.pop_front() {
        None => Err(ParseUriPathError::Split),
        Some(first) => if !first.is_empty() {
            Err(ParseUriPathError::StartSlash)
        } else {
            match uri_path.pop_front() {
                None => Err(ParseUriPathError::NoInfoHash),
                Some(info_hash) => match decode_info_hash(info_hash) {
                    Err(_) => Err(ParseUriPathError::InvalidInfoHash),
                    Ok(info_hash) => {
                        let path: Result<Vec<_>, _> = Result::from_iter(
                            uri_path
                                .into_iter()
                                .map(|s| urlencoding::decode(s)
                                    .map(std::borrow::Cow::into_owned)
                                )
                        );
                        match path {
                            Err(_) => Err(ParseUriPathError::FromUtf8),
                            Ok(path) => Ok((info_hash, path)),
                        }
                    },
                },
            }
        },
    }
}



#[derive(Debug, thiserror::Error)]
pub enum CallError<A, E> {
    #[error("error when sending an argument: {0}")]
    Sender(mpsc::error::SendError<A>),

    #[error("error when receiving a result: {0}")]
    Receiver(oneshot::error::RecvError),

    #[error("error returned by the callee: {0}")]
    Callee(E),
}

/// Makes a call through synchronization channels.
/// Sends `arg(resp_sender)` to `arg_sender` and receives a result
/// from `resp_receiver` where `resp_sender` and `resp_receiver`
/// are parts of the same oneshot channel.
pub async fn channel_call<A, R, E>(
    arg_sender: mpsc::Sender<A>,
    arg: impl FnOnce(oneshot::Sender<Result<R, E>>) -> A,
) -> Result<R, CallError<A, E>> {
    let (resp_tx, resp_rx) = oneshot::channel();
    let () = arg_sender.send(arg(resp_tx)).await.map_err(CallError::Sender)?;
    resp_rx.await.map_err(CallError::Receiver)?.map_err(CallError::Callee)
}



type CallDiskError = CallError<DiskMsg, SliceError>;

type ReadOutput = Result<Bytes, CallDiskError>;

/// The purpose of this trait is to give a name
/// to the return type of [`SliceReader::read`] (future).
/// The name is `<SliceReader as SliceReaderI>::ReadFuture`.
trait SliceReaderI {
    type ReadFuture: Future<Output = ReadOutput>;

    fn read(&self) -> Self::ReadFuture;
}

struct SliceReader {
    disk_sender: mpsc::Sender<DiskMsg>,
    id: SliceId,
}

impl Drop for SliceReader {
    /// Removes the slice from [`vincenzo::disk::Disk`].
    fn drop(&mut self) {
        let slice_id = self.id;
        let disk_sender = self.disk_sender.clone();
        /* Since this method is not `async`, we have no choice
        other than `tokio::task::spawn`. */
        let _ = tokio::task::spawn(async move {
            match channel_call(
                disk_sender,
                |recipient| DiskMsg::DropSlice { slice_id, recipient },
            ).await {
                Ok(()) => {},
                Err(error) => warn!(?error, slice_id, "when dropping a slice"),
            }
            debug!(slice_id, "the slice has been dropped")
        });
    }
}

impl SliceReaderI for SliceReader {
    type ReadFuture = impl Future<Output = ReadOutput>;

    /// Removes and returns a prefix of the slice by calling [`vincenzo::disk::Disk`].
    fn read(&self) -> Self::ReadFuture {
        let slice_id = self.id;
        let disk_sender = self.disk_sender.clone();
        async move {
            channel_call(
                disk_sender,
                |recipient| DiskMsg::ReadSlice { slice_id, recipient },
            ).await
        }
    }
}

/// A wrapper for the part of the API of [`vincenzo::disk::Disk`] needed
/// by this module.
#[derive(Clone)]
struct ContentServer {
    disk_sender: mpsc::Sender<DiskMsg>,
}

impl ContentServer {
    fn new(disk_sender: mpsc::Sender<DiskMsg>) -> Self {
        ContentServer { disk_sender }
    }

    async fn file_by_path(
        &self,
        info_hash: InfoHash,
        path: Vec<String>
    ) -> Result<FileByPathR, CallDiskError> {
        channel_call(
            self.disk_sender.clone(),
            |recipient| DiskMsg::FileByPath { info_hash, path, recipient },
        ).await
    }

    async fn new_slice(
        &self,
        file_id: FileId,
        range_in_file: Range<u64>,
    ) -> Result<SliceReader, CallDiskError> {
        let r = channel_call(
            self.disk_sender.clone(),
            |recipient| DiskMsg::NewSlice { file_id, range_in_file, recipient },
        ).await;
        r.map(|slice_id| SliceReader {
            id: slice_id,
            disk_sender: self.disk_sender.clone(),
        })
    }
}

/// Transfers torrent content in a slice of a torrent file
/// from [`vincenzo::disk::Disk`] to Hyper.
/// It takes content via [`ContentBody::slice_reader`].
/// Taking content reduces the slice.
/// It gives content by implementing [`http_body::Body`].
#[pin_project]
struct ContentBody {
    slice_reader: SliceReader,

    /// This is `Some(future)` iff there is an outstanding request
    /// to [`vincenzo::disk::Disk`]. In this case,
    /// the output of `future` will be the response to this request.
    /// See `<ContentBody as http_body::Body>::poll_frame`
    /// to understand how this is used.
    #[pin] read_future: Option<<SliceReader as SliceReaderI>::ReadFuture>,

    /// The remaiining size of the slice.
    rem_size: u64,
}

impl ContentBody {
    async fn new(
        cs: ContentServer,
        file_id: FileId,
        range_in_file: Range<u64>,
    ) -> Result<Self, CallDiskError> {
        let rem_size = range_from::range_len(range_in_file.clone());
        cs.new_slice(file_id, range_in_file).await.map(|slice_reader| {
            Self { slice_reader, read_future: None, rem_size }
        })
    }

    fn handle_resp(
        self: std::pin::Pin<&mut Self>,
        resp: ReadOutput,
    ) -> Option<
        Result<
            http_body::Frame<<ContentBody as http_body::Body>::Data>,
            <ContentBody as http_body::Body>::Error,
        >
    >
    {
        let mut selfp = self.project();
        selfp.read_future.set(None);
        Some(resp.map(|mut a| {
            let size = u64::try_from(a.len()).unwrap();
            let rem_size = selfp.rem_size;
            match rem_size.checked_sub(size) {
                None => {
                    warn!(
                        size,
                        rem_size,
                        "the HTTP server received a block of content \
                            larger than the remaining amount of content",
                    );
                    a.truncate((*rem_size).try_into().unwrap());
                    *rem_size = 0;
                },
                Some(new_rem_size) => {
                    debug!(size, new_rem_size, "ContentBody received content");
                    *rem_size = new_rem_size;
                },
            }
            http_body::Frame::data(a)
        }))
    }
}

impl http_body::Body for ContentBody {
    type Data = Bytes;
    type Error = CallDiskError;

    /// Removes and returns a prefix of the slice
    /// by calling the [`SliceReaderI::read`] method
    /// of the [`ContentBody::slice_reader`] field.
    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let selfp = self.as_mut().project();
        let mut read_future = selfp.read_future;
        match read_future.as_mut().as_pin_mut() {
            None => if *selfp.rem_size == 0 { Poll::Ready(None) } else {
                read_future.as_mut().set(Some(selfp.slice_reader.read()));
                read_future
                    .as_pin_mut()
                    .unwrap()
                    .poll(cx)
                    .map(|resp| self.handle_resp(resp))
            },
            Some(read_future) => read_future
                .poll(cx)
                .map(|resp| self.handle_resp(resp)),
        }
    }

    fn size_hint(&self) -> http_body::SizeHint {
        http_body::SizeHint::with_exact(self.rem_size)
    }
}




type EmptyBody<D> = http_body_util::Empty<D>;

fn empty_resp<S: TryInto<StatusCode>, D: bytes::Buf>(
    status_code: S,
) -> Result<Response<EmptyBody<D>>, http::Error>
where <S as TryInto<StatusCode>>::Error: Into<http::Error>
{
    my_resp_builder()
        .status(status_code)
        .body(EmptyBody::<D>::new())
}

fn binary_content_type(a: response::Builder) -> response::Builder {
    a.header(http::header::CONTENT_TYPE, "application/octet-stream")
}

type MyResp = Response<MyBody>;

fn my_resp_builder() -> response::Builder {
    Response::builder()
        .version(http::Version::HTTP_11)
        .header(http::header::ACCEPT_RANGES, "bytes")
}

/// HTTP response body.
/// It may be empty (for errors) or contain torrent content.
type MyBody = http_body_util::Either<EmptyBody<Bytes>, ContentBody>;

fn from_empty_body<E>(
    a: Result<Response<EmptyBody<Bytes>>, E>,
) -> Result<MyResp, E> {
    a.map(|resp| resp.map(http_body_util::Either::Left))
}

fn from_content_body<E>(
    a: Result<Response<ContentBody>, E>,
) -> Result<MyResp, E> {
    a.map(|resp| resp.map(http_body_util::Either::Right))
}

fn my_empty_resp<S: TryInto<StatusCode>>(
    status_code: S,
) -> Result<MyResp, http::Error>
where <S as TryInto<StatusCode>>::Error: Into<http::Error>
{
    from_empty_body(empty_resp(status_code))
}

fn resp_500(error: impl Debug) -> Result<MyResp, http::Error> {
    error!(?error, "StatusCode::INTERNAL_SERVER_ERROR");
    my_empty_resp(StatusCode::INTERNAL_SERVER_ERROR)
}

fn bad_request(error: impl Debug) -> Result<MyResp, http::Error> {
    info!(?error, "StatusCode::BAD_REQUEST");
    my_empty_resp(StatusCode::BAD_REQUEST)
}

/// If `a` is `Err(e)`, sends a 500 (Internal Server Error) response.
/// If `a` is `Ok(b)`, sends `f(b)`.
async fn map_err_to_500<T, RFuture>(
    a: Result<T, impl Debug>,
    f: impl FnOnce(T) -> RFuture,
) -> Result<MyResp, http::Error>
where RFuture: Future<Output = Result<MyResp, http::Error>>
{
    match a {
        Err(e) => resp_500(e),
        Ok(a) => f(a).await,
    }
}

async fn service_path_range(
    cs: ContentServer,
    uri_path: &str,
    rh: Option<http_range_header::ParsedRanges>,
) -> Result<MyResp, http::Error> {
    match parse_uri_path(uri_path) {
        Err(e) => match e {
            ParseUriPathError::Split => resp_500(e),
            ParseUriPathError::StartSlash => resp_500(e),
            ParseUriPathError::NoInfoHash => bad_request(e),
            ParseUriPathError::InvalidInfoHash => bad_request(e),
            ParseUriPathError::FromUtf8 => bad_request(e),
        },
        Ok((info_hash, path)) => match cs.file_by_path(info_hash, path).await {
            Err(error) => {
                info!(?error, "StatusCode::NOT_FOUND");
                my_empty_resp(StatusCode::NOT_FOUND)
            },
            Ok(FileByPathR { i: file_i, len: file_length }) => {
                let file_id = FileId { info_hash, i: file_i };
                match rh {
                    None => map_err_to_500(Range::try_from_range(..file_length),
                        |range| async move {
                            map_err_to_500(ContentBody::new(cs, file_id, range).await,
                                |content_body| async move {
                                    debug!("serving content with StatusCode::OK");
                                    from_content_body(
                                        binary_content_type(
                                            my_resp_builder().status(StatusCode::OK)
                                        )
                                        .body(content_body)
                                    )
                                }
                            ).await
                        }
                    ).await,
                    Some(rh) => match rh_in_file(file_length, rh) {
                        Err(e) => match e {
                            RhInFileError::RangeNotOne => resp_500(e),
                            RhInFileError::RangeUnsatisfiable(error) => {
                                info!(?error, "StatusCode::RANGE_NOT_SATISFIABLE");
                                /* See https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/416 . */
                                from_empty_body(
                                    my_resp_builder()
                                        .status(StatusCode::RANGE_NOT_SATISFIABLE)
                                        .header(
                                            http::header::CONTENT_RANGE,
                                            ContentRange::Unsatisfied {
                                                complete_length: file_length,
                                            }
                                            .to_string(),
                                        )
                                        .body(EmptyBody::<Bytes>::new())
                                )
                            },
                            RhInFileError::RangeConvert(e) => resp_500(e),
                        },
                        Ok(range) => map_err_to_500(RangeInclusive::try_from_range(range.clone()),
                            |range_inclusive| async move {
                                map_err_to_500(ContentBody::new(cs, file_id, range).await,
                                    |content_body| async move {
                                        debug!("serving content with StatusCode::PARTIAL_CONTENT");
                                        from_content_body(
                                            binary_content_type(
                                                my_resp_builder()
                                                    .status(StatusCode::PARTIAL_CONTENT)
                                                    .header(
                                                        http::header::CONTENT_RANGE,
                                                        ContentRange::Satisfied {
                                                            range: range_inclusive,
                                                            complete_length: Some(file_length),
                                                        }
                                                        .to_string(),
                                                    )
                                            )
                                            .body(content_body)
                                        )
                                    }
                                ).await
                            }
                        ).await,
                    },
                }
            },
        },
    }
}

/// Services one HTTP request.
async fn service<B>(
    cs: ContentServer,
    req: Request<B>,
) -> Result<MyResp, http::Error> {
    let (request::Parts { method, uri, headers, .. }, _) = req.into_parts();
    debug!(?method, ?uri, ?headers, "HTTP request");
    match method {
        http::Method::GET => {
            let is_scheme_valid = match uri.scheme() {
                None => true,
                Some(scheme) => scheme == &http::uri::Scheme::HTTP,
            };
            if !is_scheme_valid {
                info!("invalid URI scheme");
                my_empty_resp(StatusCode::NOT_FOUND)
            } else if uri.query().is_some() {
                info!("the URI query must be empty");
                my_empty_resp(StatusCode::NOT_FOUND)
            } else {
                match parse_rh(&headers) {
                    None => service_path_range(cs, uri.path(), None).await,
                    Some(a) => match a {
                        Err(e) => bad_request(e),
                        Ok(ranges) => service_path_range(cs, uri.path(), Some(ranges)).await,
                    },
                }
            }
        },
        http::Method::HEAD => resp_500("the HEAD HTTP method is not implemented") /* TODO */,
        _ => my_empty_resp(StatusCode::NOT_IMPLEMENTED),
    }
}

fn log_hyper_error_properties(error: &hyper::Error) {
    debug!(
        is_body_write_aborted = error.is_body_write_aborted(),
        is_canceled = error.is_canceled(),
        is_closed = error.is_closed(),
        is_incomplete_message = error.is_incomplete_message(),
        is_parse = error.is_parse(),
        is_parse_status = error.is_parse_status(),
        is_parse_too_large = error.is_parse_too_large(),
        is_timeout = error.is_timeout(),
        is_user = error.is_user(),
        "[`hyper::Error`] properties",
    );
}

/// The HTTP server.
/// It listens on `tcp_listener`.
/// It takes torrent content by communicating through `disk_sender`.
pub async fn main(
    tcp_listener: tokio::net::TcpListener,
    disk_sender: mpsc::Sender<DiskMsg>,
    shutdown_signal: impl Future,
    shutdown_timeout: std::time::Duration,
) {
    info!(address = ?tcp_listener.local_addr(), "the HTTP server is listening");
    let cs = ContentServer::new(disk_sender);
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();
    let mut pinned_shutdown_signal = std::pin::pin!(shutdown_signal);
    loop {
        tokio::select! {
             r = tcp_listener.accept() => {
                match r {
                    Err(error) => {
                        /* See https://utcc.utoronto.ca/~cks/space/blog/unix/AcceptErrnoProblem
                        and https://stackoverflow.com/questions/76955978/which-socket-accept-errors-are-fatal .
                        TODO: Exit the loop in case of a permanent server error. */
                        info!(?error, "accept: error when accepting an HTTP connection");
                    }
                    Ok((stream, _)) => {
                        let cs = cs.clone();
                        let io = hyper_util::rt::TokioIo::new(stream);
                        let conn = http1::Builder::new()
                            .serve_connection(io, service_fn(move |req| service(cs.clone(), req)));
                        let watched_conn = graceful.watch(conn);
                        tokio::task::spawn(async move {
                            if let Err(error) = watched_conn.await {
                                warn!(?error, "after serving an HTTP connection");
                                log_hyper_error_properties(&error);
                            }
                        });
                    },
                }
            },
            _ = &mut pinned_shutdown_signal => { break; },
        }
    }
    tokio::select! {
        _ = graceful.shutdown() => {},
        _ = tokio::time::sleep(shutdown_timeout) => {
            info!("timed out waiting for all connections to close");
        }
    }
}
