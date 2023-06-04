package indexer.core

import indexer.core.internal.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import java.nio.file.Path

fun CoroutineScope.launchIndex(
    dir: Path,
    cfg: IndexConfig,
): Index {
    val userRequests = Channel<UserRequest>()
    val indexUpdateRequests = Channel<IndexUpdateRequest>()
    val statusUpdates = Channel<StatusUpdate>(Int.MAX_VALUE)
    val fileEvents = Channel<FileEvent>(Int.MAX_VALUE)
    val statusFlow = MutableSharedFlow<IndexStateUpdate>(
        replay = 1,
        onBufferOverflow = BufferOverflow.DROP_OLDEST,
    )
    statusFlow.tryEmit(IndexStateUpdate.Initializing(System.currentTimeMillis()))

    val deferred = async {
        launch(CoroutineName("watcher")) { watcher(cfg, dir, fileEvents, statusUpdates) }
        repeat(4) {
            launch(CoroutineName("indexer-$it")) { indexer(cfg, fileEvents, indexUpdateRequests) }
        }
        launch(CoroutineName("index")) {
            index(cfg, userRequests, indexUpdateRequests, statusUpdates, statusFlow)
        }
    }

    deferred.invokeOnCompletion {
        statusFlow.tryEmit(
            IndexStateUpdate.Failed(
                System.currentTimeMillis(),
                it
                    ?: IllegalStateException("Index terminated without exception?")
            )
        )
    }

    return object : Index, Deferred<Any?> by deferred {
        override suspend fun status(): IndexStatus {
            return withIndexContext {
                val future = CompletableDeferred<IndexStatus>()
                userRequests.send(StatusRequest(future))
                future.await()
            } ?: IndexStatus.broken()
        }

        override suspend fun statusFlow(): Flow<IndexStateUpdate> {
            return flow {
                statusFlow
                    .onEach { emit(it) }
                    .takeWhile { it !is IndexStateUpdate.Failed }
                    .collect()
            }
        }

        override suspend fun findFileCandidates(query: String): Flow<FileAddress> {
            return withIndexContext {
                val result = CompletableDeferred<Flow<FileAddress>>()
                val request = FindRequest(
                    query = query,
                    result = result,
                )
                userRequests.send(request)
                result.await()
            } ?: flowOf()
        }

        // future.await() may get stuck if index gets canceled while message is inflight
        private suspend fun <T : Any> withIndexContext(
            block: suspend CoroutineScope.() -> T
        ): T? = coroutineScope {
            try {
                withContext(deferred) {
                    block()
                }
            } catch (e: CancellationException) {
                this@coroutineScope.ensureActive()
                null
            }
        }
    }
}
