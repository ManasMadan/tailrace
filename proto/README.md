# Protocol definitions

Canonical protobuf contracts published by walcast.

- [`walcast/v1/sink.proto`](./walcast/v1/sink.proto) — the
  `walcast.v1.WalcastSink` service that `@walcast/sink-grpc` pushes to.
  Implement it in any language; a runnable Node reference lives in
  [`examples/grpc-consumer`](../examples/grpc-consumer).

The same file ships inside the `@walcast/sink-grpc` npm package (its tests
assert the copies are byte-identical). Changes here are semver-relevant for
that package: additive fields are minor, anything else is major.
