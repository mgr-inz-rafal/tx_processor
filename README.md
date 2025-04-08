# Transaction Processor

This is a toy, async-based transaction processor.

## Usage

```
cargo run -- input.csv
```

Output will be emitted to `stdout`.

## Notes

There are still a couple of `TODO`s left in the code in the places that could potentially be improved.

### Assumptions

The system works with a couple of assumptions.

- Balances can never be negative.
- Account which is `locked` can not process any transactions.
- Only `Deposit` transactions can be disputed.
- Single transaction can be put under dispute again, even if it was disputed previously.
- Strings representing the transactions type in the input file are case insensitive (e.g. "Deposit" and "deposit" are treated in the same way)

### Limitations

- Error handling is implemented, but in order not to pollute the `stdout`, this is just in form of commented out `tracing` lines. Hence, transactions that lead to incorrect state (balance underflow) or inputs that are incorrect (deposit without amount) are silently ignored.
- There is an unlimited time window for the disputes to be raised. This could lead to internal storage overflow. A stub for supporting the pruning system is prepared.
- There's a separate task to manage each client state, there are pros & cons to this, but it may not scale well. Comment in the `struct StreamProcessor` explain the potential mitigation strategies.
- No test for deposit overflow (issues when trying to deserialize `Decimal::MAX` from `.csv` via `serde`) - this would require some workaround with String

## Tests

There are a couple of unit tests scattered around the different modules to check code mechanics.

The heavy lifting is done in the `tests` module which contains a bunch of scenario based tests. These have a form of input CSV file with the corresponding expected output. The file name describes the idea behind each scenario. Scenarios are split into directories for added clarity.
