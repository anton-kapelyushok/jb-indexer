package indexer.core

import indexer.core.internal.FileAddress
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.flow.Flow
import java.util.concurrent.atomic.AtomicBoolean

interface Index : Deferred<Any?> {
    suspend fun findFilesByToken(query: String): List<FileAddress>
    suspend fun findTokensMatchingPredicate(predicate: (token: String) -> Boolean): List<String>
    suspend fun state(): IndexState
    suspend fun statusFlow(): Flow<IndexStatusUpdate>
    suspend fun compact()
}

interface SearchEngine : Deferred<Any?> {
    suspend fun indexState(): IndexState
    suspend fun indexStatusUpdates(): Flow<IndexStatusUpdate>
    suspend fun find(query: String): Flow<IndexSearchResult>
    suspend fun cancelAll(cause: CancellationException? = null)
}

interface IndexConfig {
    val enableLogging: AtomicBoolean

    val enableWatcher: Boolean

    // transforms line into tokens for index to store
    fun tokenize(line: String): List<String>

    // returns possible files that match query
    suspend fun find(
        index: Index,
        query: String,
    ): Flow<FileAddress>

    // determines if line matches query
    fun matches(line: String, query: String): Boolean

    // watcher is restarted on error, exception is passed to indexStatusUpdates flow
    // however this flow has limited buffer and error might be lost
    // you can use this function to make sure you catch this exception
    suspend fun handleWatcherError(e: Throwable)

    // initial file sync can throw if directory content changes during iterating
    // it gets retried immediately, but you might want to log this error
    suspend fun handleInitialFileSyncError(e: Throwable)
}

data class IndexSearchResult(val path: String, val lineNo: Int, val line: String)

data class IndexState(
    val clock: Long,

    var startTime: Long,
    var lastRestartTime: Long,

    val indexedFiles: Int,
    val knownTokens: Int,
    val watcherStarted: Boolean,
    val handledFileEvents: Long,
    val totalFileEvents: Long,
    val isBroken: Boolean,
    val allFileDiscovered: Boolean,
) {

    fun isInSync(): Boolean {
        return !isBroken && allFileDiscovered && handledFileEvents == totalFileEvents
    }

    companion object {
        fun broken() = IndexState(
            clock = -1L,
            isBroken = true,

            indexedFiles = 0,
            knownTokens = 0,
            watcherStarted = false,
            handledFileEvents = 0L,
            totalFileEvents = 0L,
            startTime = -1L,
            lastRestartTime = -1L,
            allFileDiscovered = false,
        )
    }
}

/**
 * State transitions:
 *
 *
 * Initial -> Initializing -> [ WatcherStarted ] -> AllFilesDiscovered -> IndexInSync -> IndexOutOfSync-+
 *                                       ^                                      ^                       |
 *                                       |                                      |                       |
 *            ReinitializingBecauseFileSyncFailed                               +-----------------------+                                                                                                     |
 *
 *
 * + Failed - terminal state, can be reached from any state
 */
sealed interface IndexStatusUpdate {
    val ts: Long

    data class Initial(override val ts: Long) : IndexStatusUpdate
    data class Initializing(override val ts: Long) : IndexStatusUpdate

    data class WatcherStarted(override val ts: Long, val status: IndexState) : IndexStatusUpdate
    data class AllFilesDiscovered(override val ts: Long, val status: IndexState) : IndexStatusUpdate

    data class IndexInSync(override val ts: Long, val status: IndexState) : IndexStatusUpdate
    data class IndexOutOfSync(override val ts: Long, val status: IndexState) : IndexStatusUpdate

    data class ReinitializingBecauseFileSyncFailed(override val ts: Long, val reason: Throwable) : IndexStatusUpdate

    data class Failed(override val ts: Long, val reason: Throwable) : IndexStatusUpdate
}
