use futures::future::{ok, Either};
use futures::sync::oneshot::Canceled;
use futures::Async;
use futures::Future;
use futures::Poll;
use qutex::FutureGuard;
use qutex::Qutex;
use std::error::Error as StdError;
use std::fmt;
use std::time::Duration;
use std::time::Instant;

#[derive(Debug)]
pub enum Error<E: StdError> {
    Canceled(Canceled),
    Other(E),
}
impl<E> Error<E>
where
    E: StdError,
{
    fn from_canceled(e: Canceled) -> Self {
        Error::Canceled(e)
    }
}
impl<E> From<E> for Error<E>
where
    E: StdError,
{
    fn from(e: E) -> Self {
        Error::Other(e)
    }
}
impl<E> fmt::Display for Error<E>
where
    E: StdError,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Canceled(e) => fmt::Display::fmt(&e, f),
            Error::Other(e) => fmt::Display::fmt(&e, f),
        }
    }
}
impl<E> StdError for Error<E> where E: StdError {}

pub struct Pool<T, F: Fn() -> U, U: Future<Item = T, Error = E>, E: StdError> {
    inner: Qutex<(Vec<T>, usize)>,
    gen: F,
    cap: usize,
}
impl<T, F, U, E> Pool<T, F, U, E>
where
    F: Fn() -> U,
    U: Future<Item = T, Error = E>,
    E: StdError,
{
    pub fn new(cap: usize, gen: F) -> Self {
        Pool {
            inner: Qutex::new((Vec::new(), 0)),
            gen,
            cap,
        }
    }

    pub fn get<'a>(
        &'a self,
        timeout: Duration,
    ) -> impl Future<Item = PoolGuard<'a, T, F, U, E>, Error = Error<E>> + 'a {
        self.inner
            .clone()
            .lock()
            .map_err(Error::from_canceled)
            .and_then(move |mut l| {
                l.1 += 1;
                match l.0.pop() {
                    Some(item) => Either::A(Either::A(ok(PoolGuard { pool: &self, item }))),
                    None => {
                        if l.1 <= self.cap {
                            Either::A(Either::B(
                                (self.gen)()
                                    .map_err(|e| e.into())
                                    .map(move |item| PoolGuard { pool: &self, item }),
                            ))
                        } else {
                            Either::B(PoolFuture::new(&self, timeout))
                        }
                    }
                }
            })
    }

    pub fn add(&self, item: T) -> impl Future<Item = (), Error = Canceled> {
        self.inner.clone().lock().map(|mut l| l.0.push(item))
    }
}

enum PoolFutureInternal<T, U: Future<Item = T, Error = E>, E: StdError> {
    Guard(FutureGuard<(Vec<T>, usize)>),
    Gen(U),
}

struct PoolFuture<'a, T, F: Fn() -> U, U: Future<Item = T, Error = E>, E: StdError> {
    pool: &'a Pool<T, F, U, E>,
    internal: PoolFutureInternal<T, U, E>,
    timeout: Duration,
    start: Instant,
}
impl<'a, T, F, U, E> PoolFuture<'a, T, F, U, E>
where
    F: Fn() -> U,
    U: Future<Item = T, Error = E>,
    E: StdError,
{
    fn new(pool: &'a Pool<T, F, U, E>, timeout: Duration) -> Self {
        PoolFuture {
            pool,
            internal: PoolFutureInternal::Guard(pool.inner.clone().lock()),
            timeout,
            start: Instant::now(),
        }
    }
}
impl<'a, T, F, U, E> Future for PoolFuture<'a, T, F, U, E>
where
    F: Fn() -> U,
    U: Future<Item = T, Error = E>,
    E: StdError,
{
    type Item = PoolGuard<'a, T, F, U, E>;
    type Error = Error<E>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.internal {
            PoolFutureInternal::Guard(ref mut guard) => match guard.poll() {
                Ok(Async::Ready(mut l)) => match l.0.pop() {
                    Some(item) => Ok(Async::Ready(PoolGuard {
                        pool: &self.pool,
                        item,
                    })),
                    None => {
                        if self.start.elapsed() > self.timeout {
                            if l.1 > 0 {
                                l.1 -= 1;
                            }
                            self.internal = PoolFutureInternal::Gen((self.pool.gen)());
                            Ok(Async::NotReady)
                        } else {
                            self.internal =
                                PoolFutureInternal::Guard(self.pool.inner.clone().lock());
                            Ok(Async::NotReady)
                        }
                    }
                },
                Ok(Async::NotReady) => Ok(Async::NotReady),
                Err(e) => Err(Error::from_canceled(e)),
            },
            PoolFutureInternal::Gen(ref mut gen) => match gen.poll() {
                Ok(Async::Ready(item)) => Ok(Async::Ready(PoolGuard {
                    pool: &self.pool,
                    item,
                })),
                Ok(Async::NotReady) => Ok(Async::NotReady),
                Err(e) => Err(e.into()),
            },
        }
    }
}

#[must_use]
pub struct PoolGuard<'a, T, F: Fn() -> U, U: Future<Item = T, Error = E>, E: StdError> {
    pool: &'a Pool<T, F, U, E>,
    item: T,
}
impl<'a, T, F, U, E> PoolGuard<'a, T, F, U, E>
where
    F: Fn() -> U,
    U: Future<Item = T, Error = E>,
    E: StdError,
{
    #[must_use]
    pub fn release(self) -> impl Future<Item = (), Error = Canceled> + 'a {
        self.pool.inner.clone().lock().and_then(|mut l| {
            l.1 -= 1;
            self.pool.add(self.item)
        })
    }
}

impl<'a, T, F, U, E> std::ops::Deref for PoolGuard<'a, T, F, U, E>
where
    F: Fn() -> U,
    U: Future<Item = T, Error = E>,
    E: StdError,
{
    type Target = T;

    fn deref(&self) -> &T {
        &self.item
    }
}
impl<'a, T, F, U, E> std::ops::DerefMut for PoolGuard<'a, T, F, U, E>
where
    F: Fn() -> U,
    U: Future<Item = T, Error = E>,
    E: StdError,
{
    fn deref_mut(&mut self) -> &mut T {
        &mut self.item
    }
}
