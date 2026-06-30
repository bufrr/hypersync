# Code Review

## Findings

1. **High - timed `read_exact` calls ignore inner I/O errors.**  
   Several timeout-wrapped reads only check `.await.is_err()`, which catches elapsed time but treats `Ok(Err(_))` as success. For example, `serve_client_blocks` can continue with a zeroed response header if the upstream closes before sending one, then write a bogus `00 00 00 00 00` frame to the node instead of trying the next peer (`src/main.rs:708`, `src/main.rs:725`). The same pattern exists for node requests and gateway greetings (`src/main.rs:670`, `src/main.rs:681`, `src/main.rs:1053`). These should match `Ok(Ok(_))` explicitly and treat everything else as return/continue.

2. **High - peer-controlled frame lengths can force very large allocations.**  
   `fetch_bootstrap` accepts any frame length up to 2 GB, allocates `vec![0u8; len]`, then copies it into the accumulated bootstrap blob (`src/main.rs:763`, `src/main.rs:766`, `src/main.rs:801`). A bad or buggy upstream can therefore drive multi-GB transient allocations and potentially OOM the gateway. `serve_client_blocks` also allocates up to 200 MB per response (`src/main.rs:714`, `src/main.rs:718`). Consider mode-specific hard limits plus chunked reads directly into the destination buffer where huge frames are expected.

3. **Medium - `--push` live merge does not reconnect shadow peers.**  
   `serve_push` starts one task per non-active peer and exits that task permanently when `pump_merge` returns (`src/main.rs:1392`, `src/main.rs:1402`). Since public peers can drop TCP streams, push mode silently degrades from `n_live` sources to fewer sources until the node reconnects. The comments describe persistent multi-source acceleration, so each shadow source should probably reconnect with backoff.

4. **Medium - `--retain` / `BlockBuf` is advertised but unused.**  
   `gateway` parses `--retain` and logs it (`src/main.rs:218`, `src/main.rs:923`), creates a `BlockBuf` (`src/main.rs:915`), and comments say it is populated by live feeders (`src/main.rs:946`). In reality `serve_client_blocks` names the argument `_buf` and never reads or writes it (`src/main.rs:657`, `src/main.rs:659`). Either remove the flag/comments or implement the buffer; right now users can tune `--retain` without changing behavior.

5. **Medium - routable peer filtering drops the whole public `172.0.0.0/8` range.**  
   `is_routable` rejects every address starting with `172.` (`src/main.rs:526`, `src/main.rs:530`). Only `172.16.0.0/12` is RFC1918 private space, so this excludes valid public peers such as `172.64.x.x`. `peerd.sh` is narrower and only filters `172.18` (`peerd.sh:16`), so the Rust gateway can discard peers the daemon intentionally kept.

6. **Medium - documented CLI modes do not match the parser.**  
   The README documents `relay <host>` and `serve <port> <peers>` (`README.md:17`, `README.md:18`). The code parses `relay` as `<port> <upstream>`, so `hypersync relay 1.2.3.4` ignores `1.2.3.4` and uses the default upstream (`src/main.rs:178`, `src/main.rs:180`). There is also no `serve` subcommand; the default mode is selected by passing a numeric port as arg 1 (`src/main.rs:223`). This can make documented commands run against the wrong peer.

7. **Low - `HL_NODE` is declared but not used by `peerd.sh`.**  
   The script exposes `NODE="${HL_NODE:-hyperliquid-node-1}"` (`peerd.sh:9`) but still hardcodes `hyperliquid-node-1` in `docker logs` (`peerd.sh:15`). Operators setting `HL_NODE` will not actually change the harvested container.

## Test And Tooling Notes

- `cargo test` passes, but there are 0 tests. It reports three unused `mut` warnings in `src/main.rs`.
- `cargo clippy --all-targets --all-features` passes with warnings, including the unused `mut`s, `io_other_error`, `manual_is_multiple_of`, and an `unexpected_cfgs` warning from `hotpath::main`.
- `cargo fmt --check` fails; `src/main.rs` is not rustfmt-formatted.

## Testing Gaps

The riskiest missing coverage is around protocol error handling and parser behavior: timeout-wrapped read failures, malformed frame lengths, `read_node_peers` filtering, CLI argument parsing, and parity between `block_round` and `block_round_full` on generated or captured LZ4 payloads.
