use csv_async::{AsyncReaderBuilder, AsyncSerializer};
use csv_diff::{csv::Csv, csv_diff::CsvByteDiff};
use futures_util::StreamExt;
use rust_decimal::Decimal;
use std::{
    fs::{self, File},
    io::{BufReader, Cursor, Read},
    path::{Path, PathBuf},
};
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::{InputRecord, OutputRecord, StreamProcessor};

fn files_matching_pattern_from_dir<P: AsRef<Path>>(dir: P, pattern: &str) -> Vec<PathBuf> {
    if let Ok(entries) = fs::read_dir(&dir) {
        entries
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if path.is_file() && path.extension()?.to_str()? == pattern {
                    Some(path)
                } else {
                    None
                }
            })
            .collect()
    } else {
        panic!(
            "Unable to read test scenarios from directory: {:?}",
            dir.as_ref()
        )
    }
}

const SCENARIOS_PATH: &str = "./src/tests/scenarios";
const EXPECTED_SCENARIO_COUNT: usize = 20;

#[tokio::test]
async fn scenarios() {
    // TODO: This test is a bit messy, but good for now.
    // TODO: Scenarios could be run in parallel.
    let mut count = 0;
    for path in files_matching_pattern_from_dir(SCENARIOS_PATH, "in") {
        let file = tokio::fs::File::open(&path)
            .await
            .expect("should read scenario file")
            .compat();
        let mut csv_reader = AsyncReaderBuilder::new()
            .has_headers(true)
            .trim(csv_async::Trim::All)
            .create_deserializer(file);
        let mut input = csv_reader.deserialize::<InputRecord<Decimal>>();

        let mut stream_processor = StreamProcessor::new();
        let mut results = stream_processor.process(&mut input).await; // ?;

        let mut buffer = Vec::new();
        {
            let mut writer = AsyncSerializer::from_writer(&mut buffer);

            while let Some(client_state) = results.next().await {
                let record: OutputRecord<Decimal> = client_state.into();
                writer
                    .serialize(&record)
                    .await
                    .expect("should serialize output record");
            }
            writer.flush().await.expect("should flush writer");
        }
        let actual = String::from_utf8(buffer).expect("valid utf8 string");
        dbg!(&actual);

        let expected_path = path.with_extension("out");
        let expected_file = File::open(&expected_path).expect("should read expected file");
        let expected_file_reader: Box<dyn Read + Send> = Box::new(BufReader::new(expected_file));
        let expected = Csv::with_reader(expected_file_reader);

        let actual_cursor = Cursor::new(actual);
        let actual_cursor_reader: Box<dyn Read + Send> = Box::new(actual_cursor);
        let actual = Csv::with_reader(actual_cursor_reader);

        let csv_diff = CsvByteDiff::new().expect("should create csv diff");
        let diff_iterator = csv_diff.diff(expected, actual);

        let diffs = diff_iterator.collect::<Vec<_>>();
        assert!(diffs.is_empty(), "mismatch in scenario: {:?}", path);

        count += 1;
    }
    assert_eq!(
        count, EXPECTED_SCENARIO_COUNT,
        "incorrect number of scenarios tested"
    );
}
