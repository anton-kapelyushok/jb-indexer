// Benchmark harness; rust/src/bin/bench.rs is the identical Rust harness.
//
// Usage: BenchKt DIR word|trigram WATCHER(true|false) QUERIES_FILE ITERATIONS CONCURRENCY

import indexer.core.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first
import java.io.File
import java.lang.management.ManagementFactory
import kotlin.io.path.Path

private fun ms(t0: Long) = (System.nanoTime() - t0) / 1_000_000.0

private fun rssKb(): Pair<Long, Long> {
    val lines = File("/proc/self/status").readLines()
    fun field(name: String) = lines.firstOrNull { it.startsWith(name) }
        ?.split(Regex("\\s+"))?.getOrNull(1)?.toLongOrNull() ?: 0L
    return field("VmRSS:") to field("VmHWM:")
}

private fun jsonStr(s: String) = buildString {
    append('"')
    for (c in s) when {
        c == '"' -> append("\\\"")
        c == '\\' -> append("\\\\")
        c.code < 0x20 -> append("\\u%04x".format(c.code))
        else -> append(c)
    }
    append('"')
}

private suspend fun runQuery(se: SearchEngine, q: String): Pair<Int, Long> {
    var count = 0
    var checksum = 0L
    se.find(q).collect {
        count++
        checksum += it.lineNo + it.path.length
    }
    return count to checksum
}

fun main(args: Array<String>) = runBlocking(Dispatchers.Default) {
    val (dir, mode) = args
    val watcher = args[2] == "true"
    val queries = File(args[3]).readLines().filter { it.isNotEmpty() }
    val iters = args[4].toInt()
    val conc = args[5].toInt()
    val cfg = if (mode == "trigram") trigramIndexConfig(enableWatcher = watcher)
    else wordIndexConfig(enableWatcher = watcher)

    val t0 = System.nanoTime()
    val index = launchIndex(Path(dir), cfg)
    val se = launchSearchEngine(cfg, index)
    var allDiscoveredMs = -1.0
    val inSync = se.indexStatusUpdates().first {
        if (it is IndexStatusUpdate.AllFilesDiscovered) allDiscoveredMs = ms(t0)
        it is IndexStatusUpdate.IndexInSync || it is IndexStatusUpdate.Failed
    }
    if (inSync is IndexStatusUpdate.Failed) throw inSync.reason
    val indexMs = ms(t0)
    val state = se.indexState()
    val rssAfterIndex = rssKb().first
    System.gc()
    val heapAfterGcKb = ManagementFactory.getMemoryMXBean().heapMemoryUsage.used / 1024

    val qjson = queries.map { q ->
        val t = System.nanoTime()
        val (count, checksum) = runQuery(se, q)
        val cold = ms(t)
        val times = (0 until iters).map {
            val t1 = System.nanoTime()
            runQuery(se, q)
            ms(t1)
        }.sorted()
        val median = times.getOrNull(times.size / 2) ?: cold
        """{"q":${jsonStr(q)},"count":$count,"checksum":$checksum,"cold_ms":${"%.2f".format(cold)},"median_ms":${"%.2f".format(median)}}"""
    }

    val t = System.nanoTime()
    (0 until conc).map { async { queries.forEach { runQuery(se, it) } } }.awaitAll()
    val concMs = ms(t)
    val (rssEnd, hwm) = rssKb()

    println(
        """{"impl":"kotlin","mode":"$mode","watcher":$watcher,"index_ms":${"%.1f".format(indexMs)},""" +
                """"all_files_discovered_ms":${"%.1f".format(allDiscoveredMs)},"indexed_files":${state.indexedFiles},"known_tokens":${state.knownTokens},""" +
                """"rss_after_index_kb":$rssAfterIndex,"heap_after_gc_kb":$heapAfterGcKb,"rss_end_kb":$rssEnd,"peak_rss_kb":$hwm,""" +
                """"concurrent_ms":${"%.1f".format(concMs)},"concurrent_qps":${"%.1f".format(conc * queries.size / (concMs / 1000.0))},""" +
                """"queries":[${qjson.joinToString(",")}]}"""
    )
    se.cancelAll()
}
