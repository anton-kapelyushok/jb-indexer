| config | metric | Kotlin | Rust | Rust vs Kotlin |
|---|---|---:|---:|---:|
| word, watcher off | time to IndexInSync | 4611 ms | 836 ms | **5.5x** faster |
| word, watcher off | process peak RSS | 138 MB | 37 MB | **3.7x** less |
| word, watcher off | concurrent throughput (8 clients) | 13.5 q/s | 23.7 q/s | **1.8x** more |
| word, watcher off | process wall (JVM/binary start → exit) | 20.4 s | 8.0 s | **2.5x** faster |
| word, watcher on | time to IndexInSync | 4945 ms | 862 ms | **5.7x** faster |
| word, watcher on | process peak RSS | 139 MB | 40 MB | **3.5x** less |
| word, watcher on | concurrent throughput (8 clients) | 15.5 q/s | 24.2 q/s | **1.6x** more |
| word, watcher on | process wall (JVM/binary start → exit) | 19.3 s | 8.0 s | **2.4x** faster |
| trigram, watcher off | time to IndexInSync | 5971 ms | 2786 ms | **2.1x** faster |
| trigram, watcher off | process peak RSS | 210 MB | 60 MB | **3.5x** less |
| trigram, watcher off | concurrent throughput (8 clients) | 16.0 q/s | 24.7 q/s | **1.5x** more |
| trigram, watcher off | process wall (JVM/binary start → exit) | 18.2 s | 9.7 s | **1.9x** faster |
| trigram, watcher on | time to IndexInSync | 6256 ms | 2305 ms | **2.7x** faster |
| trigram, watcher on | process peak RSS | 199 MB | 64 MB | **3.1x** less |
| trigram, watcher on | concurrent throughput (8 clients) | 16.2 q/s | 28.0 q/s | **1.7x** more |
| trigram, watcher on | process wall (JVM/binary start → exit) | 18.8 s | 9.2 s | **2.0x** faster |


**Per-query latency, word index (median of 5 warm runs, median over 3 processes)**

| query | matches | Kotlin cold ms | Rust cold ms | Kotlin warm ms | Rust warm ms | warm speedup |
|---|---:|---:|---:|---:|---:|---:|
| `VirtualFile` | 4753 | 157.2 | 16.3 | 78.9 | 15.2 | 5.2x |
| `getInstance()` | 786 | 78.9 | 25.1 | 75.5 | 25.5 | 3.0x |
| `private static final Logger LOG` | 210 | 30.0 | 12.2 | 23.9 | 12.1 | 2.0x |
| `ApplicationManager.getApplication()` | 402 | 44.0 | 9.4 | 24.4 | 9.3 | 2.6x |
| `@NotNull String` | 2454 | 107.9 | 39.2 | 115.1 | 37.8 | 3.0x |
| `return null;` | 1352 | 145.0 | 58.3 | 155.9 | 55.2 | 2.8x |
| `TODO` | 274 | 43.6 | 17.2 | 36.2 | 17.1 | 2.1x |
| `xyzzy_definitely_absent` | 0 | 8.6 | 0.7 | 7.3 | 0.7 | 10.8x |
| `ReadAction.compute` | 27 | 29.0 | 4.4 | 27.3 | 4.7 | 5.8x |
| `ConcurrentHashMap<` | 115 | 17.5 | 5.9 | 14.6 | 6.6 | 2.2x |
| `Disposer.register(` | 139 | 14.1 | 4.7 | 11.9 | 4.5 | 2.6x |
| `if (` | 21681 | 153.8 | 96.2 | 125.2 | 74.3 | 1.7x |
| **all 12 queries** | | | | **696** | **263** | **2.6x** |

**Per-query latency, trigram index (median of 5 warm runs, median over 3 processes)**

| query | matches | Kotlin cold ms | Rust cold ms | Kotlin warm ms | Rust warm ms | warm speedup |
|---|---:|---:|---:|---:|---:|---:|
| `VirtualFile` | 4753 | 153.5 | 17.4 | 61.1 | 16.2 | 3.8x |
| `getInstance()` | 786 | 73.2 | 35.9 | 66.6 | 34.4 | 1.9x |
| `private static final Logger LOG` | 210 | 34.1 | 14.3 | 29.7 | 14.2 | 2.1x |
| `ApplicationManager.getApplication()` | 402 | 28.4 | 11.7 | 24.0 | 11.6 | 2.1x |
| `@NotNull String` | 2454 | 62.0 | 34.7 | 59.5 | 34.2 | 1.7x |
| `return null;` | 1352 | 64.4 | 36.0 | 64.0 | 36.3 | 1.8x |
| `TODO` | 274 | 29.6 | 16.6 | 23.7 | 16.3 | 1.5x |
| `xyzzy_definitely_absent` | 0 | 0.9 | 0.0 | 0.1 | 0.0 | 12.0x |
| `ReadAction.compute` | 27 | 13.0 | 6.2 | 11.7 | 6.1 | 1.9x |
| `ConcurrentHashMap<` | 115 | 12.6 | 6.6 | 11.8 | 6.6 | 1.8x |
| `Disposer.register(` | 139 | 12.5 | 8.5 | 12.8 | 8.5 | 1.5x |
| `if (` | 21681 | 623.6 | 66.6 | 124.0 | 66.0 | 1.9x |
| **all 12 queries** | | | | **489** | **250** | **2.0x** |

Kotlin JVM heap after full GC once indexed (live data): word 19 MB, trigram 35 MB
All query result counts + checksums identical across implementations and runs: OK
