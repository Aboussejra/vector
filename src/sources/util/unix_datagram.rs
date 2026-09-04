use std::{
    fs::remove_file,
    io::{self, IoSliceMut},
    os::fd::AsRawFd,
    path::PathBuf,
};

use bytes::Bytes;
use futures::StreamExt;
use nix::sys::socket::{MsgFlags, MultiHeaders, recvmmsg};
use tokio::net::UnixDatagram;
use tracing::field;
use vector_lib::{
    EstimatedJsonEncodedSizeOf,
    codecs::{DecoderFramedRead, StreamDecodingError},
    internal_event::{ByteSize, BytesReceived, InternalEventHandle as _, Protocol},
};

use crate::{
    SourceSender,
    codecs::Decoder,
    event::Event,
    internal_events::{
        SocketEventsReceived, SocketMode, SocketReceiveError, StreamClosedError,
        UnixSocketFileDeleteError,
    },
    shutdown::ShutdownSignal,
    sources::{
        Source,
        util::{change_socket_permissions, unix::UNNAMED_SOCKET_HOST},
    },
};

/// Returns a `Source` object corresponding to a Unix domain datagram socket.
/// Passing in different functions for `decoder` and `handle_events` can allow
/// for different source-specific logic (such as decoding syslog messages in the
/// syslog source).
#[allow(clippy::too_many_arguments)]
pub fn build_unix_datagram_source(
    listen_path: PathBuf,
    socket_file_mode: Option<u32>,
    max_length: usize,
    nr_buffers: usize,
    decoder: Decoder,
    handle_events: impl Fn(&mut [Event], Option<Bytes>) + Clone + Send + Sync + 'static,
    shutdown: ShutdownSignal,
    out: SourceSender,
) -> crate::Result<Source> {
    Ok(Box::pin(async move {
        let socket = UnixDatagram::bind(&listen_path).expect("Failed to bind to datagram socket");
        info!(message = "Listening.", path = ?listen_path, r#type = "unix_datagram", nr_buffers = nr_buffers, max_length = max_length);

        change_socket_permissions(&listen_path, socket_file_mode)
            .expect("Failed to set socket permissions");

        let result = listen(
            socket,
            max_length,
            nr_buffers,
            decoder,
            shutdown,
            handle_events,
            out,
        )
        .await;

        // Delete socket file.
        if let Err(error) = remove_file(&listen_path) {
            emit!(UnixSocketFileDeleteError {
                path: &listen_path,
                error
            });
        }

        result
    }))
}

async fn listen(
    socket: UnixDatagram,
    max_length: usize,
    nr_buffers: usize,
    decoder: Decoder,
    mut shutdown: ShutdownSignal,
    handle_events: impl Fn(&mut [Event], Option<Bytes>) + Clone + Send + Sync + 'static,
    mut out: SourceSender,
) -> Result<(), ()> {
    let mut buffers = vec![vec![0u8; max_length]; nr_buffers];
    let bytes_received = register!(BytesReceived::from(Protocol::UNIX));

    let span = info_span!("datagram");
    span.record("peer_path", field::debug(UNNAMED_SOCKET_HOST));
    let received_from: Bytes = socket
    .peer_addr()
    .ok()
    .and_then(|addr| {
        addr.as_pathname().map(|e| e.to_owned()).map({
            |path| {
                span.record("peer_path", field::debug(&path));
                path.to_string_lossy().into_owned().into()
            }
        })
    })
    // In most cases, we'll be connecting to this socket from
    // an unnamed socket (a socket not bound to a
    // file). Instead of a filename, we'll surface a specific
    // host value.
    .unwrap_or_else(|| UNNAMED_SOCKET_HOST.into());

    let fd = socket.as_raw_fd();

    let recvmmsg_size = metrics::gauge!("recvmmsg_size");

    loop {
        tokio::select! {
            result = socket.async_io(tokio::io::Interest::READABLE, || {
                let mut headers = MultiHeaders::<()>::preallocate(nr_buffers, None);
                let mut iovecs: Vec<[IoSliceMut; 1]> = buffers
                    .iter_mut()
                    .map(|buffer| [IoSliceMut::new(buffer)])
                    .collect();

                let lengths = recvmmsg(
                    fd,
                    &mut headers,
                    iovecs.iter_mut(),
                    MsgFlags::MSG_DONTWAIT,
                    None,
                )
                .map_err(|errno| io::Error::from_raw_os_error(errno as i32))?
                .map(|msg| msg.bytes)
                .collect::<Vec<usize>>();

                Ok(lengths)
            }) => {
                let lengths = match result {
                    Ok(lengths) => lengths,
                    Err(error) => {
                        let error = vector_lib::codecs::decoding::Error::FramingError(error.into());
                        emit!(SocketReceiveError {
                            mode: SocketMode::Unix,
                            error: &error
                        });
                        return Err(());
                    }
                };

                recvmmsg_size.set(lengths.len() as f64);

                let mut batch: Vec<Event> = Vec::with_capacity(lengths.len());

                for (buffer, &length) in buffers.iter().zip(&lengths) {
                    let data = &buffer[..length];

                    bytes_received.emit(ByteSize(data.len()));

                    let mut stream = DecoderFramedRead::new(data.as_ref(), decoder.clone());

                    while let Some(result) = stream.next().await {
                        match result {
                            Ok((mut events, _byte_size)) => {
                                emit!(SocketEventsReceived {
                                    mode: SocketMode::Unix,
                                    byte_size: events.estimated_json_encoded_size_of(),
                                    count: events.len()
                                });

                                handle_events(&mut events, Some(received_from.clone()));

                                batch.extend(events);
                            },
                            Err(error) => {
                                emit!(SocketReceiveError {
                                    mode: SocketMode::Unix,
                                    error: &error
                                });
                                if !error.can_continue() {
                                    break;
                                }
                            },
                        }
                    }
                }

                if !batch.is_empty() {
                    let count = batch.len();
                    if (out.send_batch(batch).await).is_err() {
                        emit!(StreamClosedError { count });
                    }
                }
            }
            _ = &mut shutdown => return Ok(()),
        }
    }
}
