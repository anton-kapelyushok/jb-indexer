package indexer.core

import indexer.core.internal.FileAddress
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

@OptIn(ExperimentalCoroutinesApi::class)
fun trigramIndexConfig(
    enableWatcher: Boolean = true,
    handleWatcherError: suspend (e: Throwable) -> Unit = {},
    handleInitialFileSyncError: suspend (e: Throwable) -> Unit = {},
) = object : IndexConfig {
    override val enableLogging = AtomicBoolean(false)

    override val enableWatcher: Boolean = enableWatcher

    override fun tokenize(line: String): List<String> {
        return line.lowercase().windowed(3)
    }

    override suspend fun find(
        query: String,
        index: Index,
    ) = flow<FileAddress> {
        when (query.length) {
            0 -> {
                // everything matches
                val tokens = index.findTokensMatchingPredicate { true }
                tokens
                    .asFlow()
                    .flatMapConcat { index.findFilesByToken(it).asFlow() }
                    .collect { emit(it) }

                return@flow
            }

            1, 2 -> {
                // files with trigrams containing query match
                val tokens = index
                    .findTokensMatchingPredicate { it.contains(query) }

                tokens.asFlow()
                    .flatMapConcat { index.findFilesByToken(it).asFlow() }
                    .collect { emit(it) }
                return@flow
            }
        }

        val tokens = tokenize(query)
        var fileSet = index.findFilesByToken(tokens[0]).toSet()
        for (i in 1 until tokens.size) {
            if (fileSet.isEmpty()) break
            val newFiles = index.findFilesByToken(tokens[i]).toSet()
            fileSet = fileSet.intersect(newFiles)
        }
        for (f in fileSet) emit(f)
    }

    override fun matches(line: String, query: String): Boolean {
        return line.contains(query)
    }

    override suspend fun handleWatcherError(e: Throwable) {
        handleWatcherError(e)
    }

    override suspend fun handleInitialFileSyncError(e: Throwable) {
        handleInitialFileSyncError(e)
    }
}
