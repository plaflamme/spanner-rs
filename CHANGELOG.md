<!-- next-header -->

## [Unreleased] - ReleaseDate

## [0.3.0] - 2022-09-27

### Added

* [ExecuteBatchDml](https://cloud.google.com/spanner/docs/reference/rpc/google.spanner.v1#google.spanner.v1.Spanner.ExecuteBatchDml) support through `TransactionContext::execute_updates`

## [0.2.0] - 2021-11-10

### Added

* `ToSpanner` and `FromSpanner` traits to convert to/from Rust/Spanner types
* `json` crate feature to enable `Json` Spanner type through the `serde_json` crate
* `numeric` crate feature to enable `Numeric` Spanner type through the `bigdecimal` crate
* `temporal` crate feature to enable `Timestamp` and `Date` types through the `chrono` crate

## [v0.1.1] - 2021-10-09

Initial release.

<!-- next-url -->
[Unreleased]: https://github.com/plaflamme/spanner-rs/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/plaflamme/spanner-rs/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/plaflamme/spanner-rs/compare/v0.1.1...v0.2.0
[v0.1.1]: https://github.com/plaflamme/spanner-rs/compare/fcf972a...v0.1.1