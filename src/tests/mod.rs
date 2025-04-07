use csv_async::{AsyncDeserializer, AsyncReaderBuilder, AsyncSerializer};
use csv_diff::{csv::Csv, csv_diff::CsvByteDiff};
use futures_util::{Stream, StreamExt};
use std::{
    io::{BufReader, Cursor, Read},
    path::{Path, PathBuf},
};
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};
use walkdir::WalkDir;

use crate::{
    InputCsvTransaction, NonNegativeCheckedDecimal, OutputClientData, StreamProcessor,
    client_processor::ClientState, stream_processor::Error,
};

fn files_matching_pattern_from_dir<P: AsRef<Path>>(dir: P, pattern: &str) -> Vec<PathBuf> {
    WalkDir::new(dir.as_ref())
        .into_iter()
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.is_file() && path.extension()?.to_str()? == pattern {
                Some(path.to_path_buf())
            } else {
                None
            }
        })
        .collect()
}

const SCENARIOS_PATH: &str = "./src/tests/scenarios";
const EXPECTED_SCENARIO_COUNT: usize = 25;

async fn csv_deserializer_from_file<P: AsRef<Path>>(
    path: P,
) -> AsyncDeserializer<Compat<tokio::fs::File>> {
    let file = tokio::fs::File::open(&path)
        .await
        .expect("should read scenario file")
        .compat();
    AsyncReaderBuilder::new()
        .has_headers(true)
        .trim(csv_async::Trim::All)
        .create_deserializer(file)
}

async fn result_stream_to_csv(
    mut results: impl Stream<
        Item = Result<ClientState<NonNegativeCheckedDecimal>, Error<NonNegativeCheckedDecimal>>,
    > + Unpin,
) -> Csv<Box<dyn std::io::Read + std::marker::Send>> {
    let mut buffer = Vec::new();
    {
        let mut writer = AsyncSerializer::from_writer(&mut buffer);

        while let Some(client_state) = results.next().await {
            let record: OutputClientData<NonNegativeCheckedDecimal> =
                client_state.unwrap().try_into().unwrap();
            writer
                .serialize(&record)
                .await
                .expect("should serialize output record");
        }
        writer.flush().await.expect("should flush writer");
    }
    let csv_string = String::from_utf8(buffer).expect("valid utf8 string");
    let cursor = Cursor::new(csv_string);
    let cursor_reader: Box<dyn Read + Send> = Box::new(cursor);
    Csv::with_reader(cursor_reader)
}

fn expected_csv_from_input_file(path: &Path) -> Csv<Box<dyn Read + Send>> {
    let expected_path = path.with_extension("out");
    let expected_file = std::fs::File::open(&expected_path).expect("should read expected file");
    let expected_file_reader: Box<dyn Read + Send> = Box::new(BufReader::new(expected_file));
    Csv::with_reader(expected_file_reader)
}

fn assert_csv(
    actual: Csv<Box<dyn Read + Send>>,
    expected: Csv<Box<dyn Read + Send>>,
    path: &PathBuf,
) {
    let csv_diff = CsvByteDiff::new().expect("should create csv diff");
    let diff_iterator = csv_diff.diff(expected, actual);

    let diffs = diff_iterator.collect::<Vec<_>>();
    assert!(diffs.is_empty(), "mismatch in scenario: {:?}", path);
}

#[tokio::test]
async fn scenarios() {
    // TODO: Scenarios could be run in parallel if implemented as separate tests.
    let mut count = 0;
    for path in files_matching_pattern_from_dir(SCENARIOS_PATH, "in") {
        // Read input
        let mut input = csv_deserializer_from_file(&path).await;
        let mut input_stream =
            input.deserialize::<InputCsvTransaction<NonNegativeCheckedDecimal>>();

        // Do the actual processing
        let mut stream_processor = StreamProcessor::new();
        let results_stream = stream_processor.process(&mut input_stream).await;

        // Compare results
        let actual_csv = result_stream_to_csv(results_stream).await;
        let expected_csv = expected_csv_from_input_file(&path);
        assert_csv(actual_csv, expected_csv, &path);

        count += 1;
    }
    assert_eq!(
        count, EXPECTED_SCENARIO_COUNT,
        "incorrect number of scenarios tested"
    );
}
