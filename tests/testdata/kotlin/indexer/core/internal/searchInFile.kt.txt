package indexer.core.internal

import indexer.core.IndexConfig
import indexer.core.IndexSearchResult
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.withContext
import java.io.File
import java.io.IOException

internal suspend fun searchInFile(
    cfg: IndexConfig,
    requests: ReceiveChannel<SearchInFileRequest>
) = withContext(Dispatchers.IO) {
    for (request in requests) {
        val result = request.result

        val results = try {
            File(request.fa.path)
                .readLines()
                .withIndex()
                .filter { (_, line) -> cfg.matches(line, request.query) }
                .map { (idx, line) -> IndexSearchResult(request.fa.path, idx + 1, line) }
        } catch (e: IOException) {
            cfg.debugLog("Failed to read ${request.fa.path}: $e")
            listOf()
        }

        result.complete(results)
    }
}