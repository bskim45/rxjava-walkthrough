package ktplay

import io.reactivex.Flowable
import io.reactivex.Single
import io.reactivex.functions.BiFunction
import ktplay.util.Helpers
import org.junit.Test

import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

/**
 * Operators for working with multiple streams
 *
 *
 */
class Part03MergingStreams : BaseTestObservables() {

    /**
     * Zip operator operates sort of like a zipper in the sense that it takes an event from one stream and waits
     * for an event from another other stream. Once an event for the other stream arrives, it uses the zip function
     * to merge the two events.
     *
     *
     * This is an useful scenario when for example you want to make requests to remote services in parallel and
     * wait for both responses before continuing.
     *
     *
     * Zip operator besides the streams to zip, also takes as parameter a function which will produce the
     * combined result of the zipped streams once each stream emitted it's value
     */
    @Test
    fun zipUsedForTakingTheResultOfCombinedAsyncOperations() {
        val isUserBlockedStream = Single.fromFuture(CompletableFuture.supplyAsync {
            Helpers.sleepMillis(200)
            false
        })
        val userCreditScoreStream = Single.fromFuture(CompletableFuture.supplyAsync {
            Helpers.sleepMillis(2300)
            200
        })

        val userCheckStream = Single.zip<Boolean, Int, Pair<Boolean, Int>>(isUserBlockedStream, userCreditScoreStream,
                BiFunction { isBlocked, creditScore -> Pair(isBlocked, creditScore) })
        subscribeWithLogOutputWaitingForComplete(userCheckStream)
    }

    /**
     * Implementing a periodic emitter, by waiting for a slower stream to emit periodically.
     * Since the zip operator need a pair of events, the slow stream will work like a timer by periodically emitting
     * with zip setting the pace of emissions downstream.
     */
    @Test
    fun zipUsedToSlowDownAnotherStream() {
        val colors = Flowable.just("red", "green", "blue")
        val timer = Flowable.interval(2, TimeUnit.SECONDS)

        val periodicEmitter = Flowable.zip<String, Long, String>(colors, timer, BiFunction { key, _ -> key })

        subscribeWithLogOutputWaitingForComplete(periodicEmitter)
    }


    /**
     * Merge operator combines one or more stream and passes events downstream as soon
     * as they appear
     *
     *
     * The subscriber will receive both color strings and numbers from the Observable.interval
     * as soon as they are emitted
     */
    @Test
    fun mergeOperator() {
        log.info("Starting")

        val colors = periodicEmitter("red", "green", "blue", 2, TimeUnit.SECONDS)

        val numbers = Flowable.interval(1, TimeUnit.SECONDS)
                .take(5)

        val flowable = Flowable.merge(colors, numbers)
        subscribeWithLogOutputWaitingForComplete(flowable)
    }

    /**
     * Concat operator appends another streams at the end of another
     * The ex. shows that even the 'numbers' streams should start early, the 'colors' stream emits fully its events
     * before we see any 'numbers'.
     * This is because 'numbers' stream is actually subscribed only after the 'colors' complete.
     * Should the second stream be a 'hot' emitter, its events would be lost until the first one finishes
     * and the seconds stream is subscribed.
     */
    @Test
    fun concatStreams() {
        log.info("Starting")
        val colors = periodicEmitter("red", "green", "blue", 2, TimeUnit.SECONDS)

        val numbers = Flowable.interval(1, TimeUnit.SECONDS)
                .take(4)

        val observable = Flowable.concat(colors, numbers)
        subscribeWithLogOutputWaitingForComplete(observable)
    }

    /**
     * combineLatest pairs events from multiple streams, but instead of waiting for an event
     * from other streams, it uses the last emitted event from that stream
     */
    @Test
    fun combineLatest() {
        log.info("Starting")

        val colors = periodicEmitter("red", "green", "blue", 3, TimeUnit.SECONDS)
        val numbers = Flowable.interval(1, TimeUnit.SECONDS).take(4)
        val combinedFlowables = Flowable.combineLatest(colors, numbers,
                BiFunction<String, Long, Pair<String, Long>> { key, value -> Pair(key, value) })

        subscribeWithLogOutputWaitingForComplete(combinedFlowables)
    }
}
