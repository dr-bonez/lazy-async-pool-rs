use crossbeam_channel::bounded;
use crossbeam_channel::unbounded;
use crossbeam_channel::Receiver;
use crossbeam_channel::Sender;
use futures::future::{ok, Either};
use futures::Async;
use futures::Future;
use futures::Poll;
use std::error::Error as StdError;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
#[cfg(feature = "timeout")]
use std::time::Duration;
#[cfg(feature = "timeout")]
use std::time::Instant;

struct PoolInternal<T, F: Fn() -> U, U: Future<Item = T, Error = E>, E: StdError> {
    sender: Sender<T>,
    receiver: Receiver<T>,
    out: AtomicUsize,
    gen: F,
    cap: usize,
}

/// Lazy Asyncronous Object Pool
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
    /// Generates a new pool.
    ///
    /// # Arguments
    /// * `cap` - Maximum number of objects to generate.
    ///   * Use `0` to make it unbounded.
    ///   * The `timeout` feature can cause it to go past this limit, but extra resources will be dropped when released.
    pub fn new(cap: usize, gen: F) -> Self {
        let (sender, receiver) = if cap > 0 { bounded(cap) } else { unbounded() };
        Pool(Arc::new(PoolInternal {
            sender,
            receiver,
            out: AtomicUsize::new(0),
            gen,
            cap,
        }))
    }

    /// Obtain an item from the pool.
    /// If the pool is exhausted and not at capacity, generates a new item.
    /// If the pool is exhausted and at capacity, wait for a new item to be available.
    /// * If the timeout is reached before an item becomes available, generates a new one.
    #[cfg(feature = "timeout")]
    pub fn get(self, timeout: Duration) -> impl Future<Item = PoolGuard<T, F, U, E>, Error = E> {
        match self.0.receiver.try_recv() {
            Ok(t) => Either::A(Either::A(ok(PoolGuard {
                pool: self.clone(),
                item: Some(t),
            }))),
            Err(_) => {
                let closure = |pool| {
                    move |t| PoolGuard {
                        pool,
                        item: Some(t),
                    }
                };
                if self.0.cap > 0 {
                    let out = self.0.out.load(Ordering::SeqCst);
                    if out < self.0.cap {
                        self.0.out.fetch_add(1, Ordering::SeqCst);
                        Either::A(Either::B((self.0.gen)().map(closure(self.clone()))))
                    } else {
                        Either::B(PoolFuture::new(self.clone(), timeout))
                    }
                } else {
                    Either::A(Either::B((self.0.gen)().map(closure(self.clone()))))
                }
            }
        }
    }

    /// Obtain an item from the pool.
    /// If the pool is exhausted and not at capacity, generates a new item.
    /// If the pool is exhausted and at capacity, wait for a new item to be available.
    #[cfg(not(feature = "timeout"))]
    pub fn get(self) -> impl Future<Item = PoolGuard<T, F, U, E>, Error = E> {
        match self.0.receiver.try_recv() {
            Ok(t) => Either::A(Either::A(ok(PoolGuard {
                pool: self.clone(),
                item: Some(t),
            }))),
            Err(_) => {
                let closure = |pool| {
                    move |t| PoolGuard {
                        pool,
                        item: Some(t),
                    }
                };
                if self.0.cap > 0 {
                    let out = self.0.out.load(Ordering::SeqCst);
                    if out < self.0.cap {
                        self.0.out.fetch_add(1, Ordering::SeqCst);
                        Either::A(Either::B((self.0.gen)().map(closure(self.clone()))))
                    } else {
                        Either::B(PoolFuture::new(self.clone()))
                    }
                } else {
                    Either::A(Either::B((self.0.gen)().map(closure(self.clone()))))
                }
            }
        }
    }

    /// Number of items available in the pool.
    pub fn len(&self) -> usize {
        self.0.receiver.len()
    }

    /// Manually add a new item to the pool.
    /// * Does not count towards the cap.
    pub fn add(&self, item: T) -> Result<(), crossbeam_channel::SendError<T>> {
        self.0.sender.send(item)
    }
}

struct PoolFuture<T, F: Fn() -> U, U: Future<Item = T, Error = E>, E: StdError> {
    pool: Pool<T, F, U, E>,
    internal: Option<U>,
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
            internal: None,
            pool,
        }
    }

    #[cfg(feature = "timeout")]
    fn new(pool: Pool<T, F, U, E>, timeout: Duration) -> Self {
        PoolFuture {
            internal: None,
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
    type Error = E;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match &mut self.internal {
            None => match self.pool.0.receiver.try_recv() {
                Ok(t) => Ok(Async::Ready(PoolGuard {
                    pool: self.pool.clone(),
                    item: Some(t),
                })),
                Err(_) => {
                    if self.pool.0.out.load(Ordering::SeqCst) < self.pool.0.cap || {
                        #[cfg(feature = "timeout")]
                        let cond = self.start.elapsed() > self.timeout;
                        #[cfg(not(feature = "timeout"))]
                        let cond = false;
                        cond
                    } {
                        self.internal = Some((self.pool.0.gen)());
                        Ok(Async::NotReady)
                    } else {
                        Ok(Async::NotReady)
                    }
                }
            },
            Some(ref mut fut) => match fut.poll() {
                Ok(Async::Ready(t)) => Ok(Async::Ready(PoolGuard {
                    pool: self.pool.clone(),
                    item: Some(t),
                })),
                Ok(Async::NotReady) => Ok(Async::NotReady),
                Err(e) => Err(e),
            },
        }
    }
}

/// Guard around an item checked out from the pool.
/// Will return the item to the pool when dropped.
pub struct PoolGuard<T, F: Fn() -> U, U: Future<Item = T, Error = E>, E: StdError> {
    pool: Pool<T, F, U, E>,
    item: Option<T>,
}
impl<T, F, U, E> PoolGuard<T, F, U, E>
where
    F: Fn() -> U,
    U: Future<Item = T, Error = E>,
    E: StdError,
{
    /// Detaches the item from the pool so it is not returned.
    /// If the pool is bounded, will allow the pool to generate a new object to replace it.
    pub fn detach(mut self) -> T {
        let item = self.item.take();
        if self.pool.0.cap > 0 {
            self.pool.0.out.fetch_sub(1, Ordering::SeqCst);
        }
        item.unwrap()
    }

    /// Destroys the item instead of returning it to the pool.
    pub fn destroy(self) {
        self.detach();
    }
}

impl<T, F, U, E> std::ops::Deref for PoolGuard<T, F, U, E>
where
    F: Fn() -> U,
    U: Future<Item = T, Error = E>,
    E: StdError,
{
    type Target = T;

    fn deref(&self) -> &T {
        self.item.as_ref().unwrap()
    }
}
impl<T, F, U, E> std::ops::DerefMut for PoolGuard<T, F, U, E>
where
    F: Fn() -> U,
    U: Future<Item = T, Error = E>,
    E: StdError,
{
    fn deref_mut(&mut self) -> &mut T {
        self.item.as_mut().unwrap()
    }
}
impl<T, F, U, E> Drop for PoolGuard<T, F, U, E>
where
    F: Fn() -> U,
    U: Future<Item = T, Error = E>,
    E: StdError,
{
    fn drop(&mut self) {
        #[cfg(feature = "timeout")]
        {
            if self.pool.0.cap > 0 && self.pool.0.out.load(Ordering::SeqCst) > self.pool.0.cap {
                self.pool.0.out.fetch_sub(1, Ordering::SeqCst);
                return;
            }
        }
        let item = self.item.take();
        if let Some(item) = item {
            match self.pool.add(item) {
                Ok(_) => (),
                Err(_) => {
                    if self.pool.0.cap > 0 {
                        self.pool.0.out.fetch_sub(1, Ordering::SeqCst);
                    }
                }
            };
        }
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
        .clone()
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
        })
        .map(move |rows| {
            let hello: String = rows[0].get(0);
            println!("{}", hello);
            assert_eq!("hello", &hello);
            println!("len: {}", pool.len());
            assert_eq!(1, pool.len());
        });

    tokio::run(fut.map_err(|e| eprintln!("{}", e)));
}
