# Lazy Async Pool

This crate defines an object pool that operates over futures, and generates objects on the fly.

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
lazy_async_pool = "0.1.1"
```

### Example: Connecting to bitcoin rpc server

```rust
use lazy_async_pool::Pool;

fn main() {
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
```
