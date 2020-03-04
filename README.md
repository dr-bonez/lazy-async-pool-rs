# Lazy Async Pool

This crate defines an object pool that operates over futures, and generates objects on the fly.

*now updated to support async/await*

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
lazy_async_pool = "0.3.0"
```

## Features

`timeout`: set a timeout duration after which a new object will be generated even if the pool has no free resources and is at capacity.

### Example: DB Connection Pool

```rust
use lazy_async_pool::Pool;

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

fn main() {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(test_fut())
        .unwrap()
}
```
