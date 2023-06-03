package indexer.core

import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.selects.select
import java.util.*

data class FileAddress(val path: String)

suspend fun index(
    indexRequests: ReceiveChannel<IndexRequest>,
    statusUpdates: ReceiveChannel<StatusUpdate>,
) = coroutineScope {

    val index = IndexState()
    while (true) {
        select {
            statusUpdates.onReceive { event ->
                when (event) {
                    AllFilesDiscovered -> index.handleAllFilesDiscovered()
                    WatcherStarted -> index.handleWatcherStarted()
                    is ModificationHappened -> index.handleFileDiscovered()
                    WatcherDiscoveredFileDuringInitialization -> index.handleWatcherDiscoveredFileDuringInitialization()
                }
            }
            indexRequests.onReceive { event ->
                if (enableLogging.get()) println("index: $event")

                when (event) {
                    is UpdateFileContentRequest -> index.handleUpdateFileContentRequest(event)
                    is RemoveFileRequest -> index.handleRemoveFileRequest(event)

                    // TODO: should probably be extracted
                    is FindTokenRequest -> index.handleFindTokenRequest(event)
                    is StatusRequest -> index.handleStatusRequest(event)
                }
            }

        }
    }
}

class IndexState {
    val startTime = System.currentTimeMillis()
    var watcherStartedTime: Long? = null
    var syncCompletedTime: Long? = null
    var allFilesDiscoveredTime: Long? = null

    // TODO: check if it actually works
    val fas = WeakHashMap<String, FileAddress>()
    val interner = WeakHashMap<String, String>()
    val fileUpdateTimes = WeakHashMap<FileAddress, Long>()

    val forwardIndex = mutableMapOf<FileAddress, MutableSet<String>>()
    val reverseIndex = mutableMapOf<String, MutableSet<FileAddress>>()

    var filesDiscoveredByWatcherDuringInitialization = 0L
    var totalModifications = 0L
    var handledModifications = 0L

    fun checkUpdateTime(eventTime: Long, fa: FileAddress): Unit? {
        val lastUpdate = fileUpdateTimes[fa] ?: 0L
        if (lastUpdate > eventTime) return null
        fileUpdateTimes[fa] = eventTime
        return Unit
    }

    fun handleUpdateFileContentRequest(event: UpdateFileContentRequest) {
        updateModificationsCounts()
        val path = event.path
        val fa = fas.computeIfAbsent(path) { FileAddress(it) }

        checkUpdateTime(event.t, fa) ?: return

        val tokens = event.tokens.map { token -> interner.computeIfAbsent(token) { it } }

        forwardIndex[fa]?.let { prevTokens ->
            prevTokens.forEach { reverseIndex[it]?.remove(fa) }
        }

        forwardIndex[fa] = tokens.toMutableSet()
        tokens.forEach { reverseIndex[it] = (reverseIndex[it] ?: mutableSetOf()).apply { add(fa) } }
    }

    fun handleRemoveFileRequest(event: RemoveFileRequest) {
        updateModificationsCounts()
        val path = event.path
        val fa = fas.computeIfAbsent(path) { FileAddress(it) }

        checkUpdateTime(event.t, fa) ?: return

        forwardIndex[fa]?.let { prevTokens ->
            prevTokens.forEach { reverseIndex[it]?.remove(fa) }
        }
        forwardIndex.remove(fa)
    }

    fun handleStatusRequest(event: StatusRequest) {
        event.result.complete(
            StatusResult(
                indexedFiles = forwardIndex.size,
                knownTokens = reverseIndex.size,
                watcherStartTime = watcherStartedTime?.let { it - startTime },
                initialSyncTime = syncCompletedTime?.let { it - startTime },
                handledFileModifications = handledModifications,
                totalFileModifications = if (allFilesDiscoveredTime == null) {
                    maxOf(
                        totalModifications,
                        filesDiscoveredByWatcherDuringInitialization
                    )
                } else {
                    totalModifications
                }
            )
        )
    }

    suspend fun handleFindTokenRequest(event: FindTokenRequest) {
        coroutineScope {
            val requestScope = this
            try {
                val searchTokens = tokenize(event.query)

                val onResult: suspend (FileAddress) -> Unit = { fa: FileAddress ->
                    event.onResult(fa)
                        .onFailure { e ->
                            if (enableLogging.get()) println("Search consumer failed with exception $e")
                            requestScope.cancel()
                        }
                }

                when (searchTokens.size) {
                    0 -> {
                        // index won't help us here, emit everything we have
                        forwardIndex.keys.asSequence().takeWhile { event.isConsumerAlive() }
                            .forEach { onResult(it) }
                    }

                    1 -> {
                        val query = searchTokens[0].lowercase()
                        val fullMatch = reverseIndex[query]?.asSequence() ?: sequenceOf()
                        val containsMatch = reverseIndex.entries
                            .asSequence()
                            // first isConsumerAlive - as often as possible
                            .takeWhile { event.isConsumerAlive() }
                            .filter { (token) -> token.contains(query) }
                            .flatMap { (_, fas) -> fas }

                        (fullMatch + containsMatch)
                            // second isConsumerAlive - before emitting result
                            .takeWhile { event.isConsumerAlive() }
                            .distinct()
                            .forEach { onResult(it) }
                    }

                    2 -> {
                        val (startToken, endToken) = searchTokens
                        val startFullMatch = (reverseIndex[startToken]?.asSequence() ?: sequenceOf())
                            .filter { fa ->
                                val fileTokens = forwardIndex[fa] ?: mutableSetOf()
                                endToken in fileTokens || fileTokens.any { it.startsWith(endToken) }
                            }

                        val endFullMatch = (reverseIndex[endToken]?.asSequence() ?: sequenceOf())
                            .filter { fa ->
                                val fileTokens = forwardIndex[fa] ?: mutableSetOf()
                                startToken in fileTokens || fileTokens.any { it.endsWith(startToken) }
                            }

                        val bothPartialMatch = reverseIndex.entries.asSequence()
                            .takeWhile { event.isConsumerAlive() }
                            .filter { (token) -> token.endsWith(startToken) }
                            .flatMap { (_, fas) -> fas.asSequence() }
                            .distinct()
                            .takeWhile { event.isConsumerAlive() }
                            .filter { fa -> forwardIndex[fa]?.any { it.startsWith(endToken) } ?: false }

                        (startFullMatch + endFullMatch + bothPartialMatch)
                            .takeWhile { event.isConsumerAlive() }
                            .distinct()
                            .forEach { onResult(it) }
                    }

                    else -> {
                        val startToken = searchTokens.first()
                        val endToken = searchTokens.last()
                        val coreTokens = searchTokens.subList(1, searchTokens.lastIndex)
                        coreTokens.map { reverseIndex[it] ?: setOf() }.minBy { it.size }
                            .asSequence()
                            .takeWhile { event.isConsumerAlive() }
                            .filter { fa ->
                                val fileTokens = forwardIndex[fa] ?: mutableSetOf()

                                val coreTokensMatch = coreTokens.all { it in fileTokens }
                                if (!coreTokensMatch) return@filter false

                                val startTokenMatch =
                                    startToken in fileTokens || fileTokens.any { it.endsWith(startToken) }
                                if (!startTokenMatch) return@filter false

                                val endTokenMatch =
                                    endToken in fileTokens || fileTokens.any { it.startsWith(endToken) }
                                if (!endTokenMatch) return@filter false

                                return@filter true
                            }
                            .takeWhile { event.isConsumerAlive() }
                            .distinct()
                            .forEach { onResult(it) }
                    }
                }
            } catch (e: Throwable) {
                event.onError(e)
            } finally {
                event.onFinish()
            }
        }
    }

    fun handleWatcherStarted() {
        watcherStartedTime = System.currentTimeMillis()
        println("Watcher started after ${watcherStartedTime!! - startTime} ms!")
    }

    fun handleAllFilesDiscovered() {
        allFilesDiscoveredTime = System.currentTimeMillis()
        println("All files discovered after ${allFilesDiscoveredTime!! - startTime} ms!")
    }

    fun handleFileDiscovered() {
        totalModifications++
    }

    fun handleWatcherDiscoveredFileDuringInitialization() {
        filesDiscoveredByWatcherDuringInitialization++
    }

    private fun updateModificationsCounts() {
        handledModifications++
        if (allFilesDiscoveredTime != null && syncCompletedTime == null && handledModifications == totalModifications) {
            syncCompletedTime = System.currentTimeMillis()
            println("Initial sync completed after ${syncCompletedTime!! - startTime} ms!")
        }
    }
}