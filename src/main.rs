use std::fmt::Write;

use eyre::Result;
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use itertools::Itertools;
use rand::Rng;

const ITEM_COUNT: u64 = 10_000_000;

fn main() -> Result<()> {
    let client = redis::Client::open("redis://localhost:6379")?;
    let mut con = client.get_connection()?;

    redis::cmd("FLUSHDB").execute(&mut con);

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

    for chunk in (0..ITEM_COUNT).chunks(100).into_iter() {
        let mut pipe = redis::pipe();

        for _ in chunk {
            pipe.cmd("SET")
                .arg(uuid::Uuid::new_v4().as_bytes().to_vec())
                .arg(rand::thread_rng().gen::<i64>())
                .arg("EX")
                .arg(1024)
                .arg("NX")
                .execute(&mut con);

            pb.inc(1);
        }
        pipe.execute(&mut con);
    }

    Ok(())
}
