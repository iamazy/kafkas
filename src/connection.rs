use std::{
    collections::BTreeMap,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};

use futures::{
    channel::{mpsc, oneshot},
    future::{select, Either},
    pin_mut, Future, FutureExt, Sink, SinkExt, Stream, StreamExt,
};
use kafka_protocol::messages::{ApiVersionsRequest, RequestKind, ResponseKind};
use tracing::{debug, error, trace};

use crate::{
    client::KafkaOptions,
    error::{ConnectionError, SharedError},
    executor::{Executor, ExecutorKind},
    protocol::{Command, KafkaCodec, KafkaRequest},
    ToStrBytes,
};

pub(crate) struct RegisterPair {
    correlation_id: i32,
    resolver: oneshot::Sender<Command>,
}

#[derive(Clone)]
pub struct SerialId(Arc<AtomicUsize>);

impl Default for SerialId {
    fn default() -> Self {
        SerialId(Arc::new(AtomicUsize::new(0)))
    }
}

impl SerialId {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self) -> i32 {
        self.0.fetch_add(1, Ordering::Relaxed) as i32
    }
}

pub struct ConnectionSender<Exe: Executor> {
    tx: mpsc::UnboundedSender<Command>,
    registrations: mpsc::UnboundedSender<RegisterPair>,
    receiver_shutdown: Option<oneshot::Sender<()>>,
    correlation_id: SerialId,
    error: SharedError,
    executor: Arc<Exe>,
    operation_timeout: Duration,
    client_id: Option<String>,
}

impl<Exe: Executor> ConnectionSender<Exe> {
    pub(crate) fn new(
        tx: mpsc::UnboundedSender<Command>,
        registrations: mpsc::UnboundedSender<RegisterPair>,
        receiver_shutdown: oneshot::Sender<()>,
        error: SharedError,
        executor: Arc<Exe>,
        operation_timeout: Duration,
    ) -> Self {
        Self {
            tx,
            registrations,
            receiver_shutdown: Some(receiver_shutdown),
            correlation_id: SerialId::new(),
            error,
            executor,
            operation_timeout,
            client_id: None,
        }
    }

    pub async fn send(&self, request: RequestKind) -> Result<ResponseKind, ConnectionError> {
        let (resolver, response) = oneshot::channel();
        let response = async {
            response.await.map_err(|oneshot::Canceled| {
                self.error.set(ConnectionError::Disconnected);
                ConnectionError::Disconnected
            })
        };
        let correlation_id = self.correlation_id.get();
        let mut request = KafkaRequest::new(correlation_id, request)?;
        request.client_id(self.client_id.clone());
        match (
            self.registrations.unbounded_send(RegisterPair {
                correlation_id,
                resolver,
            }),
            self.tx.unbounded_send(Command::Request(request)),
        ) {
            (Ok(_), Ok(_)) => {
                let delay_f = self.executor.delay(self.operation_timeout);
                pin_mut!(response);
                pin_mut!(delay_f);

                match select(response, delay_f).await {
                    Either::Left((Ok(Command::Response(res)), _)) => Ok(res.response),
                    Either::Left((Err(e), _)) => Err(e),
                    Either::Left((_, _)) => Err(ConnectionError::UnexpectedResponse(
                        "receive an invalid request".into(),
                    )),
                    Either::Right(_) => Err(ConnectionError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "timeout sending message to the kafka server",
                    ))),
                }
            }
            _ => Err(ConnectionError::Disconnected),
        }
    }

    pub async fn send_oneway(&self, request: RequestKind) -> Result<(), ConnectionError> {
        let mut request = KafkaRequest::new(0, request)?;
        request.client_id(self.client_id.clone());
        self.tx
            .unbounded_send(Command::Request(request))
            .map_err(|_| ConnectionError::Disconnected)?;
        Ok(())
    }
}

struct Receiver<S: Stream<Item = Result<Command, ConnectionError>>> {
    inbound: Pin<Box<S>>,
    outbound: mpsc::UnboundedSender<Command>,
    error: SharedError,
    pending_requests: BTreeMap<i32, oneshot::Sender<Command>>,
    received_commands: BTreeMap<i32, Command>,
    registrations: Pin<Box<mpsc::UnboundedReceiver<RegisterPair>>>,
    shutdown: Pin<Box<oneshot::Receiver<()>>>,
}

impl<S: Stream<Item = Result<Command, ConnectionError>>> Receiver<S> {
    pub fn new(
        inbound: S,
        outbound: mpsc::UnboundedSender<Command>,
        error: SharedError,
        registrations: mpsc::UnboundedReceiver<RegisterPair>,
        shutdown: oneshot::Receiver<()>,
    ) -> Self {
        Receiver {
            inbound: Box::pin(inbound),
            outbound,
            error,
            pending_requests: BTreeMap::new(),
            received_commands: BTreeMap::new(),
            registrations: Box::pin(registrations),
            shutdown: Box::pin(shutdown),
        }
    }
}

impl<S: Stream<Item = Result<Command, ConnectionError>>> Future for Receiver<S> {
    type Output = Result<(), ()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.shutdown.as_mut().poll(cx) {
            Poll::Ready(Ok(())) | Poll::Ready(Err(futures::channel::oneshot::Canceled)) => {
                return Poll::Ready(Err(()));
            }
            Poll::Pending => {}
        }

        loop {
            match self.registrations.as_mut().poll_next(cx) {
                Poll::Ready(Some(RegisterPair {
                    correlation_id,
                    resolver,
                })) => match self.received_commands.remove(&correlation_id) {
                    Some(command) => {
                        let _ = resolver.send(command);
                    }
                    None => {
                        self.pending_requests.insert(correlation_id, resolver);
                    }
                },
                Poll::Ready(None) => {
                    self.error.set(ConnectionError::Disconnected);
                    return Poll::Ready(Err(()));
                }
                Poll::Pending => break,
            }
        }

        loop {
            match self.inbound.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(cmd))) => {
                    if let Command::Response(ref res) = cmd {
                        if let Some(resolver) =
                            self.pending_requests.remove(&res.header.correlation_id)
                        {
                            let _ = resolver.send(cmd);
                        }
                    }
                }
                Poll::Ready(None) => {
                    self.error.set(ConnectionError::Disconnected);
                    return Poll::Ready(Err(()));
                }
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Err(e))) => {
                    self.error.set(e);
                    return Poll::Ready(Err(()));
                }
            }
        }
    }
}

pub struct Connection<Exe: Executor> {
    id: i64,
    url: String,
    sender: ConnectionSender<Exe>,
}

impl<Exe: Executor> Connection<Exe> {
    pub async fn new(
        url: String,
        connection_timeout: Duration,
        operation_timeout: Duration,
        options: &KafkaOptions,
        executor: Arc<Exe>,
    ) -> Result<Connection<Exe>, ConnectionError> {
        debug!("connecting to {}", url);
        let sender_prepare =
            Connection::prepare_stream(&url, executor.clone(), operation_timeout, options);
        let delay_f = executor.delay(connection_timeout);

        pin_mut!(sender_prepare);
        pin_mut!(delay_f);

        let sender = match select(sender_prepare, delay_f).await {
            Either::Left((s, _)) => s?,
            Either::Right(_) => {
                return Err(ConnectionError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "timeout connecting to the kafka server.",
                )))
            }
        };

        let id = rand::random();
        let conn = Connection {
            id,
            url: url.clone(),
            sender,
        };
        conn.api_version().await?;
        Ok(conn)
    }

    async fn prepare_stream(
        address: &String,
        executor: Arc<Exe>,
        operation_timeout: Duration,
        options: &KafkaOptions,
    ) -> Result<ConnectionSender<Exe>, ConnectionError> {
        let codec = KafkaCodec::default();
        let client_id = options.client_id.clone();
        match executor.kind() {
            #[cfg(feature = "tokio-runtime")]
            ExecutorKind::Tokio => {
                let stream = tokio::net::TcpStream::connect(address)
                    .await
                    .map(|stream| tokio_util::codec::Framed::new(stream, codec))?;
                Connection::connect(stream, executor, operation_timeout, client_id).await
            }
            #[cfg(feature = "async-std-runtime")]
            ExecutorKind::AsyncStd => {
                let stream = async_std::net::TcpStream::connect(address)
                    .await
                    .map(|stream| asynchronous_codec::Framed::new(stream, codec))?;
                Connection::connect(stream, executor, operation_timeout, client_id).await
            }
            #[cfg(not(feature = "tokio-runtime"))]
            ExecutorKind::Tokio => {
                unimplemented!("the tokio-runtime cargo feature is not active.");
            }
            #[cfg(not(feature = "async-std-runtime"))]
            ExecutorKind::AsyncStd => {
                unimplemented!("the async-std-runtime cargo feature is not active.");
            }
        }
    }

    pub async fn connect<S>(
        stream: S,
        executor: Arc<Exe>,
        operation_timeout: Duration,
        client_id: Option<String>,
    ) -> Result<ConnectionSender<Exe>, ConnectionError>
    where
        S: Stream<Item = Result<Command, ConnectionError>>,
        S: Sink<Command, Error = ConnectionError>,
        S: Send + std::marker::Unpin + 'static,
    {
        let (mut sink, stream) = stream.split();
        let (tx, mut rx) = mpsc::unbounded();
        let (registrations_tx, registrations_rx) = mpsc::unbounded();
        let error = SharedError::new();
        let (receiver_shutdown_tx, receiver_shutdown_rx) = oneshot::channel();

        if executor
            .spawn(Box::pin(
                Receiver::new(
                    stream,
                    tx.clone(),
                    error.clone(),
                    registrations_rx,
                    receiver_shutdown_rx,
                )
                .map(|_| ()),
            ))
            .is_err()
        {
            error!("the executor could not spawn the receiver future");
            return Err(ConnectionError::Shutdown);
        }

        let err = error.clone();
        let res = executor.spawn(Box::pin(async move {
            while let Some(cmd) = rx.next().await {
                if let Err(e) = sink.send(cmd).await {
                    error!("error occur: {:?}", e);
                    err.set(e);
                    break;
                }
            }
        }));
        if res.is_err() {
            error!("the executor could not spawn the receiver future");
            return Err(ConnectionError::Shutdown);
        }

        let mut sender = ConnectionSender::new(
            tx,
            registrations_tx,
            receiver_shutdown_tx,
            error,
            executor,
            operation_timeout,
        );
        sender.client_id = client_id;
        Ok(sender)
    }

    pub fn sender(&self) -> &ConnectionSender<Exe> {
        &self.sender
    }

    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn is_valid(&self) -> bool {
        !self.sender.error.is_set()
    }
}

impl<Exe: Executor> Connection<Exe> {
    const PKG_VERSION: &'static str = env!("CARGO_PKG_VERSION");
    const PKG_NAME: &'static str = env!("CARGO_PKG_NAME");

    async fn api_version(&self) -> Result<(), ConnectionError> {
        let request = ApiVersionsRequest {
            client_software_name: Self::PKG_NAME.to_string().to_str_bytes(),
            client_software_version: Self::PKG_VERSION.to_string().to_str_bytes(),
            ..Default::default()
        };
        let request = RequestKind::ApiVersionsRequest(request);
        self.sender().send(request).await?;
        Ok(())
    }
}

impl<Exe: Executor> Drop for Connection<Exe> {
    fn drop(&mut self) {
        trace!("dropping connection {} for {}", self.id, self.url);
        if let Some(shutdown) = self.sender.receiver_shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}
