use std::future::Future;
use std::marker::Unpin;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
#[cfg(feature = "timeout")]
use std::time::Duration;
#[cfg(feature = "timeout")]
use std::time::Instant;

use crossbeam_channel::bounded;
use crossbeam_channel::unbounded;
use crossbeam_channel::Receiver;
use crossbeam_channel::Sender;

struct PoolInternal<T, F: Fn() -> U, U: Future<Output = Result<T, E>>, E> {
    sender: Sender<T>,
    receiver: Receiver<T>,
    out: AtomicUsize,
    gen: F,
    cap: usize,
}

/// Lazy Asyncronous Object Pool
pub struct Pool<T, F: Fn() -> U, U: Future<Output = Result<T, E>> + Unpin, E>(
    Arc<PoolInternal<T, F, U, E>>,
);
impl<T, F, U, E> Clone for Pool<T, F, U, E>
where
    F: Fn() -> U,
    U: Future<Output = Result<T, E>> + Unpin,
{
    fn clone(&self) -> Self {
        Pool(self.0.clone())
    }
}
impl<T, F, U, E> Pool<T, F, U, E>
where
    F: Fn() -> U,
    U: Future<Output = Result<T, E>> + Unpin,
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
    pub async fn get(self, timeout: Duration) -> Result<PoolGuard<T, F, U, E>, E> {
        match self.0.receiver.try_recv() {
            Ok(t) => Ok(PoolGuard {
                pool: self.clone(),
                item: Some(t),
            }),
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
                        (self.0.gen)().await.map(closure(self.clone()))
                    } else {
                        PoolFuture::new(self.clone(), timeout).await
                    }
                } else {
                    (self.0.gen)().await.map(closure(self.clone()))
                }
            }
        }
    }

    /// Obtain an item from the pool.
    /// If the pool is exhausted and not at capacity, generates a new item.
    /// If the pool is exhausted and at capacity, wait for a new item to be available.
    #[cfg(not(feature = "timeout"))]
    pub async fn get(self) -> Result<PoolGuard<T, F, U, E>, E> {
        match self.0.receiver.try_recv() {
            Ok(t) => Ok(PoolGuard {
                pool: self.clone(),
                item: Some(t),
                dirty: false,
            }),
            Err(_) => {
                let closure = |pool| {
                    move |t| PoolGuard {
                        pool,
                        item: Some(t),
                        dirty: false,
                    }
                };
                if self.0.cap > 0 {
                    let out = self.0.out.load(Ordering::SeqCst);
                    if out < self.0.cap {
                        self.0.out.fetch_add(1, Ordering::SeqCst);
                        (self.0.gen)().await.map(closure(self.clone()))
                    } else {
                        PoolFuture::new(self.clone()).await
                    }
                } else {
                    (self.0.gen)().await.map(closure(self.clone()))
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

struct PoolFuture<T, F: Fn() -> U, U: Future<Output = Result<T, E>> + Unpin, E> {
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
    U: Future<Output = Result<T, E>> + Unpin,
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
    U: Future<Output = Result<T, E>> + Unpin,
{
    type Output = Result<PoolGuard<T, F, U, E>, E>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match &mut self.internal {
            None => match self.pool.0.receiver.try_recv() {
                Ok(t) => Poll::Ready(Ok(PoolGuard {
                    pool: self.pool.clone(),
                    item: Some(t),
                    dirty: false,
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
                        Poll::Pending
                    } else {
                        Poll::Pending
                    }
                }
            },
            Some(ref mut fut) => match Pin::new(fut).poll(cx) {
                Poll::Ready(Ok(t)) => Poll::Ready(Ok(PoolGuard {
                    pool: self.pool.clone(),
                    item: Some(t),
                    dirty: false,
                })),
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

/// Guard around an item checked out from the pool.
/// Will return the item to the pool when dropped.
pub struct PoolGuard<T, F: Fn() -> U, U: Future<Output = Result<T, E>> + Unpin, E> {
    pool: Pool<T, F, U, E>,
    item: Option<T>,
    dirty: bool,
}
impl<T, F, U, E> PoolGuard<T, F, U, E>
where
    F: Fn() -> U,
    U: Future<Output = Result<T, E>> + Unpin,
{
    /// Marks a resource as being in a dirty state.
    /// If it is dropped while in a dirty state, it will be destroyed instead of returned to the pool.
    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    /// Marks a resource as no longer being dirty.
    pub fn mark_clean(&mut self) {
        self.dirty = false;
    }

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
    U: Future<Output = Result<T, E>> + Unpin,
{
    type Target = T;

    fn deref(&self) -> &T {
        self.item.as_ref().unwrap()
    }
}
impl<T, F, U, E> std::ops::DerefMut for PoolGuard<T, F, U, E>
where
    F: Fn() -> U,
    U: Future<Output = Result<T, E>> + Unpin,
{
    fn deref_mut(&mut self) -> &mut T {
        self.item.as_mut().unwrap()
    }
}
impl<T, F, U, E> Drop for PoolGuard<T, F, U, E>
where
    F: Fn() -> U,
    U: Future<Output = Result<T, E>> + Unpin,
{
    fn drop(&mut self) {
        if self.pool.0.cap > 0
            && ((cfg!(feature = "timeout")
                && self.pool.0.out.load(Ordering::SeqCst) > self.pool.0.cap)
                || self.dirty)
        {
            self.pool.0.out.fetch_sub(1, Ordering::SeqCst);
        } else {
            if let Some(item) = self.item.take() {
                match self.pool.add(item) {
                    Ok(_) => {
                        if self.pool.0.cap > 0 {
                            self.pool.0.out.fetch_sub(1, Ordering::SeqCst);
                        }
                    }
                    Err(_) => (),
                };
            }
        }
    }
}

#[cfg(test)]
async fn test_fut() -> Result<(), failure::Error> {
    use futures::FutureExt;
    use futures::TryFutureExt;

    let pool = Pool::new(20, || {
        tokio_postgres::connect(
            "postgres://amcclelland:pass@localhost:5432/pgdb",
            tokio_postgres::NoTls,
        )
        .map_ok(|(client, connection)| {
            let connection = connection.map_err(|e| eprintln!("connection error: {}", e));
            tokio::spawn(connection);
            client
        })
        .boxed()
    });

    let client = pool.clone().get().await?;
    let stmt = client.prepare("SELECT $1::TEXT").await?;
    let rows = client.query(&stmt, &[&"hello".to_owned()]).await?;
    let hello: String = rows[0].get(0);
    println!("{}", hello);
    assert_eq!("hello", &hello);
    println!("len: {}", pool.len());
    assert_eq!(1, pool.len());
    Ok(())
}

#[test]
fn test() {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(test_fut())
        .unwrap();
}
