#![feature(io_error_more)]

use std::ffi::OsString;
use std::fs::File;
use std::io::{Read, ErrorKind, Seek, SeekFrom, Write};
use std::time::Instant;

use argh::FromArgs;
use rusqlite::{Connection, OpenFlags};

/// Measure the sequential read speed in 1MiB chunks to find slow (i.e. probably reallocated or otherwise damaged) sectors.
#[derive(FromArgs)]
struct Args {
    /// write instead of reading.
    /// WARNING: This WILL destroy your data!
    #[argh(switch)]
    write: bool,

    /// path to sqlite file (will be created if it doesn't exist yet) where the test results are stored.
    #[argh(positional)]
    db_path: OsString,
    /// device to test. Typically a raw block device, but nothing prevents you from using this tool on e.g. a regular file.
    /// However, don't expect the results to be particularly useful in that case.
    #[argh(positional)]
    device_under_test: String,
}

fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;

    let Args { write, db_path, device_under_test } = argh::from_env();

    let db = Connection::open_with_flags(db_path, OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_NO_MUTEX | OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    db.execute_batch(r#"
    PRAGMA synchronous = OFF;
    CREATE TABLE IF NOT EXISTS test_runs(
        id INTEGER PRIMARY KEY NOT NULL,
        ts TEXT NOT NULL,
        is_write BOOL NOT NULL,
        disk_path TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS measurements(
        test_run INTEGER NOT NULL REFERENCES test_runs(id),
        sector_num INTEGER NOT NULL,
        access_us INTEGER NOT NULL
    );
    "#)?;

    let mut f = File::options()
        .create(false)
        .read(true)
        .write(write)
        .open(&device_under_test)?;

    db.execute("INSERT INTO test_runs(ts, is_write, disk_path) VALUES(datetime(), ?, ?)", (write, device_under_test))?;
    let test_run_id = db.last_insert_rowid();
    let mut insert_query = db.prepare("INSERT INTO measurements(test_run, sector_num, access_us) VALUES(?, ?, ?)")?;

    let mut buf = [0u8; 1024 * 1024]; // read 1MB chunks
    let mut sector_num = 0;

    f.read_exact(&mut buf)?; // make hdd spin up
    f.seek(SeekFrom::Start(0))?; // seek back so sector numbers are correct
    // TODO: could drop caches here but either way the first chunk is now in the hdd-internal cache so who cares

    let mut last_ts = Instant::now();

    buf.fill(0x33);
    write!(buf.as_mut_slice(), "This data was overwritten by find-slow-sectors because you set the --write flag. Enjoy!\r\n\0")?;

    loop {
        if write {
            match f.write_all(&buf) {
                Err(e) if e.kind() == ErrorKind::StorageFull => break,
                x => x,
            }?;
        } else {
            match f.read_exact(&mut buf) {
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                x => x,
            }?;
        }
        let now = Instant::now();
        let duration = now - last_ts;
        last_ts = now;

        let duration_ms: u64 = duration.as_micros().try_into()?;
        insert_query.execute((test_run_id, sector_num, duration_ms))?;
        sector_num += 1;
    }

    Ok(())
}
