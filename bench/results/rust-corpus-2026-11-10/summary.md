| config | metric | Kotlin | Rust | Comparison |
|---|---|---:|---:|---:|
| word, watcher off | time to IndexInSync | 11316 ms | 3298 ms | Rust **3.4x** faster |
| word, watcher off | process peak RSS | 553 MiB | 460 MiB | Rust **1.2x** less |
| word, watcher off | concurrent throughput (8 clients) | 2.6 q/s | 2.7 q/s | Rust **1.0x** more |
| word, watcher off | process wall (JVM/binary start → exit) | 79.3 s | 67.5 s | Rust **1.2x** faster |
| word, watcher on | time to IndexInSync | 13476 ms | 3670 ms | Rust **3.7x** faster |
| word, watcher on | process peak RSS | 525 MiB | 453 MiB | Rust **1.2x** less |
| word, watcher on | concurrent throughput (8 clients) | 2.6 q/s | 2.8 q/s | Rust **1.1x** more |
| word, watcher on | process wall (JVM/binary start → exit) | 82.1 s | 67.1 s | Rust **1.2x** faster |
| trigram, watcher off | time to IndexInSync | 23931 ms | 15140 ms | Rust **1.6x** faster |
| trigram, watcher off | process peak RSS | 1278 MiB | 539 MiB | Rust **2.4x** less |
| trigram, watcher off | concurrent throughput (8 clients) | 3.1 q/s | 3.4 q/s | Rust **1.1x** more |
| trigram, watcher off | process wall (JVM/binary start → exit) | 86.1 s | 67.7 s | Rust **1.3x** faster |
| trigram, watcher on | time to IndexInSync | 24336 ms | 16092 ms | Rust **1.5x** faster |
| trigram, watcher on | process peak RSS | 1275 MiB | 543 MiB | Rust **2.3x** less |
| trigram, watcher on | concurrent throughput (8 clients) | 2.7 q/s | 3.4 q/s | Rust **1.3x** more |
| trigram, watcher on | process wall (JVM/binary start → exit) | 90.0 s | 67.5 s | Rust **1.3x** faster |


**Per-query latency, word index (median of 5 warm runs, median over 3 processes)**

| query | matches | Kotlin cold ms | Rust cold ms | Kotlin warm ms | Rust warm ms | Kotlin/Rust warm latency |
|---|---:|---:|---:|---:|---:|---:|
| `fn main` | 32540 | 2191.0 | 594.9 | 956.1 | 598.2 | 1.6x |
| `pub fn` | 49021 | 751.6 | 595.0 | 690.4 | 595.9 | 1.2x |
| `impl ` | 54290 | 519.4 | 564.9 | 513.0 | 550.2 | 0.9x |
| `#[test]` | 15360 | 338.4 | 435.2 | 352.3 | 424.5 | 0.8x |
| `use std::` | 19562 | 469.1 | 445.8 | 397.3 | 451.7 | 0.9x |
| `return ` | 40568 | 367.1 | 418.6 | 378.1 | 426.4 | 0.9x |
| `TODO` | 394 | 86.7 | 72.3 | 65.8 | 64.5 | 1.0x |
| `xyzzy_definitely_absent` | 0 | 16.3 | 4.0 | 13.7 | 3.8 | 3.6x |
| `unsafe {` | 30163 | 261.1 | 288.3 | 267.1 | 291.7 | 0.9x |
| `Option<` | 28680 | 307.8 | 348.3 | 326.3 | 344.9 | 0.9x |
| `Result<` | 15774 | 336.3 | 374.4 | 331.9 | 380.5 | 0.9x |
| `if ` | 123417 | 530.3 | 611.9 | 518.2 | 613.6 | 0.8x |
| **all 12 queries** | | | | **4810** | **4746** | **1.0x** |

**Per-query latency, trigram index (median of 5 warm runs, median over 3 processes)**

| query | matches | Kotlin cold ms | Rust cold ms | Kotlin warm ms | Rust warm ms | Kotlin/Rust warm latency |
|---|---:|---:|---:|---:|---:|---:|
| `fn main` | 32540 | 4983.3 | 587.8 | 551.4 | 578.2 | 1.0x |
| `pub fn` | 49021 | 814.1 | 360.0 | 341.8 | 356.6 | 1.0x |
| `impl ` | 54290 | 1020.9 | 489.0 | 390.1 | 485.3 | 0.8x |
| `#[test]` | 15360 | 138.2 | 95.4 | 70.3 | 93.9 | 0.7x |
| `use std::` | 19562 | 732.1 | 395.0 | 323.0 | 396.0 | 0.8x |
| `return ` | 40568 | 575.3 | 433.3 | 338.4 | 441.5 | 0.8x |
| `TODO` | 394 | 97.5 | 56.0 | 42.1 | 53.7 | 0.8x |
| `xyzzy_definitely_absent` | 0 | 1.3 | 0.2 | 0.3 | 0.1 | 5.6x |
| `unsafe {` | 30163 | 408.0 | 260.1 | 199.4 | 256.7 | 0.8x |
| `Option<` | 28680 | 489.8 | 360.7 | 270.9 | 355.0 | 0.8x |
| `Result<` | 15774 | 228.0 | 204.2 | 153.7 | 205.6 | 0.7x |
| `if ` | 123417 | 1018.9 | 598.3 | 466.1 | 617.0 | 0.8x |
| **all 12 queries** | | | | **3147** | **3840** | **0.8x** |

Kotlin JVM heap after full GC once indexed (live data): word 116 MiB, trigram 183 MiB
All query result counts + checksums identical across implementations and runs: OK
