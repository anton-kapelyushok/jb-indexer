package indexer.core.internal

import indexer.core.IndexSearchResult
import indexer.core.IndexState
import kotlinx.coroutines.CompletableDeferred

internal enum class FileEventSource {
    INITIAL_SYNC,
    WATCHER,
}

internal enum class FileEventType {
    CREATE,
    DELETE,
    MODIFY,
}

internal data class FileSyncEvent(
    val t: Long,
    val fileAddress: FileAddress,
    val source: FileEventSource,
    val type: FileEventType,
)

internal sealed interface StatusUpdate {
    object WatcherStarted : StatusUpdate
    data class FileSyncFailed(val t: Long, val reason: Throwable) : StatusUpdate
    object AllFilesDiscovered : StatusUpdate
    object WatcherDiscoveredFileDuringInitialization : StatusUpdate
    data class FileUpdated(val source: FileEventSource) : StatusUpdate
}

internal sealed interface FileUpdateRequest {

    data class UpdateFile(
        val t: Long,
        val fileAddress: FileAddress,
        val tokens: Set<String>,
        val source: FileEventSource,
    ) : FileUpdateRequest {
        override fun toString(): String {
            return "UpdateFileContentRequest(t=$t, tokens=${tokens.size}, fileAddress=$fileAddress, source=$source)"
        }
    }

    data class RemoveFileRequest(
        val t: Long,
        val fileAddress: FileAddress,
        val source: FileEventSource,
    ) : FileUpdateRequest
}

internal sealed interface UserRequest

internal data class FindFilesByTokenRequest(
    val query: String,
    val result: CompletableDeferred<List<FileAddress>>,
) : UserRequest

internal data class FindTokensMatchingPredicateRequest(
    val predicate: (token: String) -> Boolean,
    val result: CompletableDeferred<List<String>>
) : UserRequest

internal data class StatusRequest(
    val result: CompletableDeferred<IndexState>
) : UserRequest

internal data class CompactRequest(
    val result: CompletableDeferred<Unit>
) : UserRequest

internal data class SearchInFileRequest(
    val fa: FileAddress,
    val query: String,
    val result: CompletableDeferred<List<IndexSearchResult>>
)