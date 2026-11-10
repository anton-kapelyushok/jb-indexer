package indexer.core.internal

import indexer.core.IndexState
import indexer.core.IndexStatusUpdate
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.withContext
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

data class FileAddress(val path: String)

internal suspend fun indexManager(
    userRequests: ReceiveChannel<UserRequest>,
    indexUpdateRequests: ReceiveChannel<FileUpdateRequest>,
    statusUpdates: ReceiveChannel<StatusUpdate>,
    emitStatusUpdate: (IndexStatusUpdate) -> Unit,
    enableDebugLog: AtomicBoolean = AtomicBoolean(false),
) = coroutineScope {
    val index = IndexManager(emitStatusUpdate, enableDebugLog)
    fun debugLog(s: String) = if (enableDebugLog.get()) println(s) else Unit

    try {
        while (true) {
            select {
                statusUpdates.onReceive { event ->
                    debugLog("statusUpdates: $event")
                    when (event) {
                        StatusUpdate.WatcherDiscoveredFileDuringInitialization -> index.handleWatcherDiscoveredFileDuringInitialization()
                        StatusUpdate.WatcherStarted -> index.handleWatcherStarted()
                        is StatusUpdate.FileUpdated -> index.handleFileUpdated()
                        StatusUpdate.AllFilesDiscovered -> index.handleAllFilesDiscovered()
                        is StatusUpdate.FileSyncFailed -> index.handleFileSyncFailed(event)
                    }
                }
                userRequests.onReceive { event ->
                    debugLog("userRequests: $event")
                    when (event) {
                        is FindFilesByTokenRequest -> index.handleFindFileByTokenRequest(event)
                        is StatusRequest -> index.handleStatusRequest(event)
                        is FindTokensMatchingPredicateRequest -> index.handleFindTokensMatchingPredicateRequest(event)
                        is CompactRequest -> index.handleCompactRequest(event)
                    }
                }
                indexUpdateRequests.onReceive { event ->
                    debugLog("indexRequests: $event")
                    when (event) {
                        is FileUpdateRequest.UpdateFile -> index.handleUpdateFileContentRequest(event)
                        is FileUpdateRequest.RemoveFileRequest -> index.handleRemoveFileRequest(event)
                    }
                }
            }
        }
    } finally {
        withContext(NonCancellable) {
            index.handleComplete()
        }
    }
}

internal class IndexManager(
    private val emitStatusUpdate: (IndexStatusUpdate) -> Unit,
    private val enableDebugLog: AtomicBoolean = AtomicBoolean(false),
    private val invertedIndex: InvertedIndex = InvertedIndex(),
) {
    private var clock = 0L
    private var logicalTimeOfLastWatcherReset = 0L
    private val startTime = System.currentTimeMillis()
    private var lastRestartTime = startTime
    private var watcherStarted: Boolean = false
    private var allFilesDiscovered: Boolean = false
    private var syncCompleted: Boolean = false
    private val fileUpdateTimes = WeakHashMap<FileAddress, Long>()
    private var filesDiscoveredByWatcherDuringInitialization = 0L
    private var totalFileEvents = 0L
    private var handledFileEvents = 0L

    fun handleUpdateFileContentRequest(event: FileUpdateRequest.UpdateFile) {
        handleFileSyncEvent(event.t, event.fileAddress) ?: return
        invertedIndex.addOrUpdateDocument(event.fileAddress, event.tokens)
    }

    fun handleRemoveFileRequest(event: FileUpdateRequest.RemoveFileRequest) {
        handleFileSyncEvent(event.t, event.fileAddress) ?: return
        invertedIndex.removeDocument(event.fileAddress)
    }

    fun handleStatusRequest(event: StatusRequest) {
        event.result.complete(status())
    }

    fun handleFindFileByTokenRequest(event: FindFilesByTokenRequest) {
        val result = event.result
        result.complete(invertedIndex.findFilesByToken(event.query))
    }

    fun handleFindTokensMatchingPredicateRequest(event: FindTokensMatchingPredicateRequest) {
        val result = event.result
        result.complete(invertedIndex.findTokensMatchingPredicate(event.predicate))
    }

    fun handleCompactRequest(event: CompactRequest) {
        invertedIndex.compact()
        event.result.complete(Unit)
    }

    fun handleWatcherStarted() {
        val watcherStartedTime = System.currentTimeMillis()
        debugLog("Watcher started after ${watcherStartedTime - startTime} ms!")
        watcherStarted = true
        emitStatusUpdate(IndexStatusUpdate.WatcherStarted(watcherStartedTime, status()))
    }

    fun handleAllFilesDiscovered() {
        val allFilesDiscoveredTime = System.currentTimeMillis()
        allFilesDiscovered = true
        debugLog("All files discovered after ${allFilesDiscoveredTime - startTime} ms!")
        emitStatusUpdate(IndexStatusUpdate.AllFilesDiscovered(allFilesDiscoveredTime, status()))
        if (handledFileEvents == totalFileEvents) {
            emitStatusUpdate(IndexStatusUpdate.IndexInSync(allFilesDiscoveredTime, status()))
        }
    }

    fun handleFileUpdated() {
        val wasInSync = status().isInSync()
        clock++
        totalFileEvents++
        if (wasInSync) {
            emitStatusUpdate(IndexStatusUpdate.IndexOutOfSync(System.currentTimeMillis(), status()))
        }
    }

    fun handleWatcherDiscoveredFileDuringInitialization() {
        filesDiscoveredByWatcherDuringInitialization++
    }

    fun handleComplete() {
        clock++
        invertedIndex.reset()
    }

    fun handleFileSyncFailed(event: StatusUpdate.FileSyncFailed) {
        clock++
        watcherStarted = false
        allFilesDiscovered = false
        lastRestartTime = System.currentTimeMillis()
        fileUpdateTimes.clear()
        invertedIndex.reset()
        filesDiscoveredByWatcherDuringInitialization = 0L
        totalFileEvents = 0L
        handledFileEvents = 0L
        logicalTimeOfLastWatcherReset = event.t

        emitStatusUpdate(
            IndexStatusUpdate.ReinitializingBecauseFileSyncFailed(
                System.currentTimeMillis(),
                event.reason
            )
        )
    }

    private fun handleFileSyncEvent(t: Long, fa: FileAddress): Unit? {
        clock++
        checkEventHappenedAfterReset(t) ?: return null

        handledFileEvents++
        if (allFilesDiscovered && handledFileEvents == totalFileEvents) {
            if (!syncCompleted) {
                val syncCompletedTime = System.currentTimeMillis()
                syncCompleted = true
                debugLog("Initial sync completed after ${syncCompletedTime - startTime} ms!")
            }
            emitStatusUpdate(IndexStatusUpdate.IndexInSync(System.currentTimeMillis(), status()))
        }

        checkLaterEventAlreadyHandled(t, fa) ?: return null

        return Unit
    }

    private fun checkEventHappenedAfterReset(eventTime: Long): Unit? {
        if (eventTime < logicalTimeOfLastWatcherReset) return null
        return Unit
    }

    private fun checkLaterEventAlreadyHandled(eventTime: Long, fa: FileAddress): Unit? {
        val lastUpdate = fileUpdateTimes[fa] ?: 0L
        if (eventTime < lastUpdate) return null
        fileUpdateTimes[fa] = eventTime
        return Unit
    }

    private fun status() = IndexState(
        clock = clock,
        indexedFiles = invertedIndex.documentsCount,
        knownTokens = invertedIndex.tokensCount,
        watcherStarted = watcherStarted,
        allFileDiscovered = allFilesDiscovered,
        handledFileEvents = handledFileEvents,
        totalFileEvents = if (!allFilesDiscovered) {
            maxOf(totalFileEvents, filesDiscoveredByWatcherDuringInitialization)
        } else {
            totalFileEvents
        },
        startTime = startTime,
        lastRestartTime = lastRestartTime,
        isBroken = false,
    )

    private fun debugLog(s: String) = if (enableDebugLog.get()) println(s) else Unit
}