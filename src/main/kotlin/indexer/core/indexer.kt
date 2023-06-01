package indexer.core

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.withContext
import kotlin.io.path.fileSize
import kotlin.io.path.readLines

suspend fun indexer(watchEvents: ReceiveChannel<WatchEvent>, indexRequests: SendChannel<IndexRequest>) {
    for (event in watchEvents) {

        if (enableLogging.get()) println("indexer: $event")
        when (event.type) {
            WatchEventType.ADDED, WatchEventType.MODIFIED -> handleUpdated(event, indexRequests)
            WatchEventType.REMOVED -> handleRemoved(event, indexRequests)
            WatchEventType.WATCHER_STARTED -> indexRequests.send(WatcherStartedMessage)
            WatchEventType.SYNC_COMPLETED -> indexRequests.send(SyncCompletedMessage)
        }
    }
}

private suspend fun handleRemoved(event: WatchEvent, indexRequests: SendChannel<IndexRequest>) {
    indexRequests.send(RemoveFileRequest(event.path))
}

private suspend fun handleUpdated(event: WatchEvent, indexRequests: SendChannel<IndexRequest>) {
    withContext(Dispatchers.IO) {
        try {
            if (event.path.fileSize() > 10_000_000L) {
                // file to large
                return@withContext
            }

            val tokens = event.path.readLines()
                .flatMap { tokenize(it) }
                .toSet()

            indexRequests.send(UpdateFileContentRequest(event.path, tokens))
        } catch (e: Throwable) {
//            println("$event")
//            e.printStackTrace()
        }
    }
}
