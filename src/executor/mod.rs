use std::{ops::Deref, pin::Pin, sync::Arc, task::Poll, time::Duration};

use futures::{Future, Stream};

#[cfg(feature = "async-std-runtime")]
mod timeout;

/// indicates which executor is used
pub enum ExecutorKind {
    /// Tokio executor
    Tokio,
    /// async-std executor
    AsyncStd,
}

/// Wrapper trait abstracting the Tokio and async-std executors
pub trait Executor: Clone + Send + Sync + 'static {
    /// spawns a new task
    #[allow(clippy::result_unit_err)]
    fn spawn(&self, f: Pin<Box<dyn Future<Output = ()> + Send>>) -> Result<(), ()>;
    /// spawns a new blocking task
    fn spawn_blocking<F, Res>(&self, f: F) -> JoinHandle<Res>
    where
        F: FnOnce() -> Res + Send + 'static,
        Res: Send + 'static;

    /// returns a Stream that will produce at regular intervals
    fn interval(&self, duration: Duration) -> Interval;
    /// waits for a configurable time
    fn delay(&self, duration: Duration) -> Delay;

    fn timeout<T>(&self, duration: Duration, fut: T) -> Timeout<T>
    where
        T: Future;

    /// returns which executor is currently used
    /// test at runtime and manually choose the implementation
    /// because we cannot (yet) have async trait methods,
    /// so we cannot move the TCP connection here
    fn kind(&self) -> ExecutorKind;
}

/// Wrapper for the Tokio executor
#[cfg(feature = "tokio-runtime")]
#[derive(Clone, Debug)]
pub struct TokioExecutor;

#[cfg(feature = "tokio-runtime")]
impl Executor for TokioExecutor {
    fn spawn(&self, f: Pin<Box<dyn Future<Output = ()> + Send>>) -> Result<(), ()> {
        tokio::task::spawn(f);
        Ok(())
    }

    fn spawn_blocking<F, Res>(&self, f: F) -> JoinHandle<Res>
    where
        F: FnOnce() -> Res + Send + 'static,
        Res: Send + 'static,
    {
        JoinHandle::Tokio(tokio::task::spawn_blocking(f))
    }

    fn interval(&self, duration: Duration) -> Interval {
        Interval::Tokio(tokio::time::interval(duration))
    }

    fn delay(&self, duration: Duration) -> Delay {
        Delay::Tokio(tokio::time::sleep(duration))
    }

    fn timeout<T>(&self, duration: Duration, fut: T) -> Timeout<T>
    where
        T: Future,
    {
        Timeout::Tokio(tokio::time::timeout(duration, fut))
    }

    fn kind(&self) -> ExecutorKind {
        ExecutorKind::Tokio
    }
}

/// Wrapper for the async-std executor
#[cfg(feature = "async-std-runtime")]
#[derive(Clone, Debug)]
pub struct AsyncStdExecutor;

#[cfg(feature = "async-std-runtime")]
impl Executor for AsyncStdExecutor {
    fn spawn(&self, f: Pin<Box<dyn Future<Output = ()> + Send>>) -> Result<(), ()> {
        async_std::task::spawn(f);
        Ok(())
    }

    fn spawn_blocking<F, Res>(&self, f: F) -> JoinHandle<Res>
    where
        F: FnOnce() -> Res + Send + 'static,
        Res: Send + 'static,
    {
        JoinHandle::AsyncStd(async_std::task::spawn_blocking(f))
    }

    fn interval(&self, duration: Duration) -> Interval {
        Interval::AsyncStd(async_std::stream::interval(duration))
    }

    fn delay(&self, duration: Duration) -> Delay {
        use async_std::prelude::FutureExt;
        Delay::AsyncStd(Box::pin(async_std::future::ready(()).delay(duration)))
    }

    fn timeout<T>(&self, duration: Duration, fut: T) -> Timeout<T>
    where
        T: Future,
    {
        Timeout::AsyncStd(timeout::Timeout::new(fut, duration))
    }

    fn kind(&self) -> ExecutorKind {
        ExecutorKind::AsyncStd
    }
}

impl<Exe: Executor> Executor for Arc<Exe> {
    fn spawn(&self, f: Pin<Box<dyn Future<Output = ()> + Send>>) -> Result<(), ()> {
        self.deref().spawn(f)
    }

    fn spawn_blocking<F, Res>(&self, f: F) -> JoinHandle<Res>
    where
        F: FnOnce() -> Res + Send + 'static,
        Res: Send + 'static,
    {
        self.deref().spawn_blocking(f)
    }

    fn interval(&self, duration: Duration) -> Interval {
        self.deref().interval(duration)
    }

    fn delay(&self, duration: Duration) -> Delay {
        self.deref().delay(duration)
    }

    fn timeout<T>(&self, duration: Duration, fut: T) -> Timeout<T>
    where
        T: Future,
    {
        self.deref().timeout(duration, fut)
    }

    fn kind(&self) -> ExecutorKind {
        self.deref().kind()
    }
}

/// future returned by [Executor::spawn_blocking] to await on the task's result
pub enum JoinHandle<T> {
    /// wrapper for tokio's `JoinHandle`
    #[cfg(feature = "tokio-runtime")]
    Tokio(tokio::task::JoinHandle<T>),
    /// wrapper for async-std's `JoinHandle`
    #[cfg(feature = "async-std-runtime")]
    AsyncStd(async_std::task::JoinHandle<T>),
    // here to avoid a compilation error since T is not used
    #[cfg(all(not(feature = "tokio-runtime"), not(feature = "async-std-runtime")))]
    PlaceHolder(T),
}

impl<T: Unpin> Future for JoinHandle<T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context) -> Poll<Self::Output> {
        match self.get_mut() {
            #[cfg(feature = "tokio-runtime")]
            JoinHandle::Tokio(j) => match Pin::new(j).poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(v) => Poll::Ready(v.ok()),
            },
            #[cfg(feature = "async-std-runtime")]
            JoinHandle::AsyncStd(j) => match Pin::new(j).poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(v) => Poll::Ready(Some(v)),
            },
            #[cfg(all(not(feature = "tokio-runtime"), not(feature = "async-std-runtime")))]
            JoinHandle::PlaceHolder(t) => {
                unimplemented!(
                    "please activate one of the following cargo features: tokio-runtime, \
                     async-std-runtime"
                )
            }
        }
    }
}

/// a `Stream` producing a `()` at rgular time intervals
pub enum Interval {
    /// wrapper for tokio's interval
    #[cfg(feature = "tokio-runtime")]
    Tokio(tokio::time::Interval),
    /// wrapper for async-std's interval
    #[cfg(feature = "async-std-runtime")]
    AsyncStd(async_std::stream::Interval),
    #[cfg(all(not(feature = "tokio-runtime"), not(feature = "async-std-runtime")))]
    PlaceHolder,
}

impl Stream for Interval {
    type Item = ();

    fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context) -> Poll<Option<Self::Item>> {
        unsafe {
            match Pin::get_unchecked_mut(self) {
                #[cfg(feature = "tokio-runtime")]
                Interval::Tokio(j) => match Pin::new_unchecked(j).poll_tick(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(_) => Poll::Ready(Some(())),
                },
                #[cfg(feature = "async-std-runtime")]
                Interval::AsyncStd(j) => match Pin::new_unchecked(j).poll_next(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(v) => Poll::Ready(v),
                },
                #[cfg(all(not(feature = "tokio-runtime"), not(feature = "async-std-runtime")))]
                Interval::PlaceHolder => {
                    unimplemented!(
                        "please activate one of the following cargo features: tokio-runtime, \
                         async-std-runtime"
                    )
                }
            }
        }
    }
}

/// a future producing a `()` after some time
#[allow(clippy::large_enum_variant)]
pub enum Delay {
    /// wrapper around tokio's `Sleep`
    #[cfg(feature = "tokio-runtime")]
    Tokio(tokio::time::Sleep),
    /// wrapper around async-std's `Delay`
    #[cfg(feature = "async-std-runtime")]
    AsyncStd(Pin<Box<dyn Future<Output = ()> + Send>>),
    #[cfg(all(not(feature = "tokio-runtime"), not(feature = "async-std-runtime")))]
    PlaceHolder,
}

impl Future for Delay {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context) -> Poll<Self::Output> {
        unsafe {
            match Pin::get_unchecked_mut(self) {
                #[cfg(feature = "tokio-runtime")]
                Delay::Tokio(d) => match Pin::new_unchecked(d).poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(_) => Poll::Ready(()),
                },
                #[cfg(feature = "async-std-runtime")]
                Delay::AsyncStd(j) => match Pin::new_unchecked(j).poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(_) => Poll::Ready(()),
                },
                #[cfg(all(not(feature = "tokio-runtime"), not(feature = "async-std-runtime")))]
                Delay::PlaceHolder => {
                    unimplemented!(
                        "please activate one of the following cargo features: tokio-runtime, \
                         async-std-runtime"
                    )
                }
            }
        }
    }
}

/// a future producing a `()` after some time
#[allow(clippy::large_enum_variant)]
pub enum Timeout<T: Future> {
    /// wrapper for tokio's `Timeout`
    #[cfg(feature = "tokio-runtime")]
    Tokio(tokio::time::Timeout<T>),
    /// wrapper for async-std's `Timeout`
    #[cfg(feature = "async-std-runtime")]
    AsyncStd(timeout::Timeout<T>),
    #[cfg(all(not(feature = "tokio-runtime"), not(feature = "async-std-runtime")))]
    PlaceHolder,
}

impl<T> Future for Timeout<T>
where
    T: Future,
{
    type Output = Result<T::Output, ()>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context) -> Poll<Self::Output> {
        unsafe {
            match Pin::get_unchecked_mut(self) {
                #[cfg(feature = "tokio-runtime")]
                Timeout::Tokio(t) => match Pin::new_unchecked(t).poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Ok(v)) => Poll::Ready(Ok(v)),
                    Poll::Ready(Err(_)) => Poll::Ready(Err(())),
                },
                #[cfg(feature = "async-std-runtime")]
                Timeout::AsyncStd(j) => match Pin::new_unchecked(j).poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Ok(v)) => Poll::Ready(Ok(v)),
                    Poll::Ready(Err(_)) => Poll::Ready(Err(())),
                },
                #[cfg(all(not(feature = "tokio-runtime"), not(feature = "async-std-runtime")))]
                Timeout::PlaceHolder => {
                    unimplemented!(
                        "please activate one of the following cargo features: tokio-runtime, \
                         async-std-runtime"
                    )
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::executor::{AsyncStdExecutor, Executor};

    #[tokio::test]
    async fn timeout() {
        let executor = AsyncStdExecutor;
        let (_tx, rx) = futures::channel::oneshot::channel::<i32>();
        // let _ = tx.send(1);
        if let Err(_) = executor.timeout(Duration::from_millis(1), rx).await {
            println!("did not receive value within 1 ms");
        }
    }
}
