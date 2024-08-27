use std::fmt::Write;

use eyre::Result;
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use r2d2_redis::{r2d2, redis};
use rand::Rng;
use rayon::prelude::*;

const ITEM_COUNT: u64 = 10_000_000;

fn main() -> Result<()> {
    let manager = r2d2_redis::RedisConnectionManager::new("redis://localhost:6379")?;
    let pool = r2d2::Pool::builder().max_size(64).build(manager)?;

    {
        let mut con = pool.get().unwrap();
        redis::cmd("FLUSHDB").execute(&mut *con);
    }

    let pb = ProgressBar::new(ITEM_COUNT);
    pb.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{human_len} ({per_second} per second) ({eta})",
        )
        .unwrap()
        .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
            write!(w, "{:.0}s", state.eta().as_secs_f64()).unwrap()
        })
        .with_key("per_second", |state: &ProgressState, w: &mut dyn Write| {
            write!(w, "{:.0}s", state.per_sec()).unwrap()
        }),
    );

    (0..ITEM_COUNT).into_par_iter().for_each(|_| {
        let mut con = pool.get().unwrap();
        redis::cmd("SET")
            .arg(uuid::Uuid::new_v4().as_bytes().to_vec())
            .arg(rand::thread_rng().gen::<i64>())
            .arg("EX")
            .arg(1024)
            .arg("NX")
            .execute(&mut *con);
        pb.inc(1);
    });

    Ok(())
}
