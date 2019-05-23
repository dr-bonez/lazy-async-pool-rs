use futures::future::{ok, Either};
use futures::sync::oneshot::Canceled;
use futures::Async;
use futures::Future;
use futures::Poll;
use qutex::FutureGuard;
use qutex::Qutex;
use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;
#[cfg(feature = "timeout")]
use std::time::Duration;
#[cfg(feature = "timeout")]
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

struct PoolInternal<T, F: Fn() -> U, U: Future<Item = T, Error = E>, E: StdError> {
    inner: Qutex<(Vec<T>, usize)>,
    gen: F,
    cap: usize,
}

pub struct Pool<T, F: Fn() -> U, U: Future<Item = T, Error = E>, E: StdError>(
    Arc<PoolInternal<T, F, U, E>>,
);
impl<T, F, U, E> Clone for Pool<T, F, U, E>
where
    F: Fn() -> U,
    U: Future<Item = T, Error = E>,
    E: StdError,
{
    fn clone(&self) -> Self {
        Pool(self.0.clone())
    }
}
impl<T, F, U, E> Pool<T, F, U, E>
where
    F: Fn() -> U,
    U: Future<Item = T, Error = E>,
    E: StdError,
{
    pub fn new(cap: usize, gen: F) -> Self {
        Pool(Arc::new(PoolInternal {
            inner: Qutex::new((Vec::new(), 0)),
            gen,
            cap,
        }))
    }

    #[cfg(feature = "timeout")]
    pub fn get(
        self,
        timeout: Duration,
    ) -> impl Future<Item = PoolGuard<T, F, U, E>, Error = Error<E>> {
        self.0
            .inner
            .clone()
            .lock()
            .map_err(Error::from_canceled)
            .and_then(move |mut l| {
                l.1 += 1;
                match l.0.pop() {
                    Some(item) => Either::A(Either::A(ok(PoolGuard {
                        pool: self.clone(),
                        item,
                    }))),
                    None => {
                        if self.0.cap > 0 && l.1 <= self.0.cap {
                            Either::A(Either::B((self.0.gen)().map_err(|e| e.into()).map(
                                move |item| PoolGuard {
                                    pool: self.clone(),
                                    item,
                                },
                            )))
                        } else {
                            Either::B(PoolFuture::new(self.clone(), timeout))
                        }
                    }
                }
            })
    }

    #[cfg(not(feature = "timeout"))]
    pub fn get(self) -> impl Future<Item = PoolGuard<T, F, U, E>, Error = Error<E>> {
        self.0
            .inner
            .clone()
            .lock()
            .map_err(Error::from_canceled)
            .and_then(move |mut l| {
                l.1 += 1;
                match l.0.pop() {
                    Some(item) => Either::A(Either::A(ok(PoolGuard {
                        pool: self.clone(),
                        item,
                    }))),
                    None => {
                        if self.0.cap > 0 && l.1 <= self.0.cap {
                            Either::A(Either::B((self.0.gen)().map_err(|e| e.into()).map(
                                move |item| PoolGuard {
                                    pool: self.clone(),
                                    item,
                                },
                            )))
                        } else {
                            Either::B(PoolFuture::new(self.clone()))
                        }
                    }
                }
            })
    }

    pub fn add(self, item: T) -> impl Future<Item = (), Error = Canceled> {
        self.0.inner.clone().lock().map(|mut l| l.0.push(item))
    }
}

enum PoolFutureInternal<T, U: Future<Item = T, Error = E>, E: StdError> {
    Guard(FutureGuard<(Vec<T>, usize)>),
    Gen(U),
}

struct PoolFuture<T, F: Fn() -> U, U: Future<Item = T, Error = E>, E: StdError> {
    pool: Pool<T, F, U, E>,
    internal: PoolFutureInternal<T, U, E>,
    #[cfg(feature = "timeout")]
    timeout: Duration,
    #[cfg(feature = "timeout")]
    start: Instant,
}
impl<'a, T, F, U, E> PoolFuture<T, F, U, E>
where
    F: Fn() -> U,
    U: Future<Item = T, Error = E>,
    E: StdError,
{
    #[cfg(not(feature = "timeout"))]
    fn new(pool: Pool<T, F, U, E>) -> Self {
        PoolFuture {
            internal: PoolFutureInternal::Guard(pool.0.inner.clone().lock()),
            pool,
        }
    }

    #[cfg(feature = "timeout")]
    fn new(pool: Pool<T, F, U, E>, timeout: Duration) -> Self {
        PoolFuture {
            internal: PoolFutureInternal::Guard(pool.0.inner.clone().lock()),
            pool,
            timeout,
            start: Instant::now(),
        }
    }
}
impl<T, F, U, E> Future for PoolFuture<T, F, U, E>
where
    F: Fn() -> U,
    U: Future<Item = T, Error = E>,
    E: StdError,
{
    type Item = PoolGuard<T, F, U, E>;
    type Error = Error<E>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.internal {
            PoolFutureInternal::Guard(ref mut guard) => match guard.poll() {
                Ok(Async::Ready(mut l)) => match l.0.pop() {
                    Some(item) => Ok(Async::Ready(PoolGuard {
                        pool: self.pool.clone(),
                        item,
                    })),
                    None => {
                        if {
                            #[cfg(feature = "timeout")]
                            let cond = self.start.elapsed() > self.timeout;
                            #[cfg(not(feature = "timeout"))]
                            let cond = false;
                            cond
                        } {
                            if l.1 > 0 {
                                l.1 -= 1;
                            }
                            self.internal = PoolFutureInternal::Gen((self.pool.0.gen)());
                            Ok(Async::NotReady)
                        } else {
                            self.internal =
                                PoolFutureInternal::Guard(self.pool.0.inner.clone().lock());
                            Ok(Async::NotReady)
                        }
                    }
                },
                Ok(Async::NotReady) => Ok(Async::NotReady),
                Err(e) => Err(Error::from_canceled(e)),
            },
            PoolFutureInternal::Gen(ref mut gen) => match gen.poll() {
                Ok(Async::Ready(item)) => Ok(Async::Ready(PoolGuard {
                    pool: self.pool.clone(),
                    item,
                })),
                Ok(Async::NotReady) => Ok(Async::NotReady),
                Err(e) => Err(e.into()),
            },
        }
    }
}

#[must_use]
pub struct PoolGuard<T, F: Fn() -> U, U: Future<Item = T, Error = E>, E: StdError> {
    pool: Pool<T, F, U, E>,
    item: T,
}
impl<T, F, U, E> PoolGuard<T, F, U, E>
where
    F: Fn() -> U,
    U: Future<Item = T, Error = E>,
    E: StdError,
{
    #[must_use]
    pub fn release(self) -> impl Future<Item = (), Error = Canceled> {
        self.pool.0.inner.clone().lock().and_then(|mut l| {
            l.1 -= 1;
            self.pool.add(self.item)
        })
    }
}

impl<'a, T, F, U, E> std::ops::Deref for PoolGuard<T, F, U, E>
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
impl<'a, T, F, U, E> std::ops::DerefMut for PoolGuard<T, F, U, E>
where
    F: Fn() -> U,
    U: Future<Item = T, Error = E>,
    E: StdError,
{
    fn deref_mut(&mut self) -> &mut T {
        &mut self.item
    }
}

#[test]
fn test() {
    use failure::Error;

    let pool = Pool::new(20, || {
        tokio_postgres::connect(
            "postgres://amcclelland:pass@localhost:5432/pgdb",
            tokio_postgres::NoTls,
        )
        .map(|(client, connection)| {
            let connection = connection.map_err(|e| eprintln!("connection error: {}", e));
            tokio::spawn(connection);
            client
        })
    });

    let fut = pool
        .get()
        .map_err(Error::from)
        .and_then(|mut client| {
            client
                .prepare("SELECT $1::TEXT")
                .map(|stmt| (client, stmt))
                .map_err(Error::from)
        })
        .and_then(move |(mut client, stmt)| {
            use futures::stream::Stream;
            client
                .query(&stmt, &[&"hello".to_owned()])
                .take(1)
                .collect()
                .map_err(Error::from)
                .map(|rows| (client, rows))
        })
        .and_then(|(client, rows)| client.release().map(move |_| rows).map_err(Error::from))
        .map(|rows| {
            let hello: String = rows[0].get(0);
            assert_eq!("hello", &hello)
        });

    tokio::run(fut.map_err(|e| eprintln!("{}", e)))
}
