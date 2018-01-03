package ktplay

import io.reactivex.BackpressureStrategy
import io.reactivex.Flowable
import io.reactivex.functions.BiFunction
import org.junit.Test

import java.util.Random
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Exceptions are for exceptional situations.
 * The Observable contract specifies that exceptions are terminal operations.
 * There are however operator available for error flow control
 */
class Part08ErrorHandling : BaseTestObservables() {

    /**
     * After the map() operator encounters an error, it triggers the error handler
     * in the subscriber which also unsubscribes from the stream,
     * therefore 'yellow' is not even sent downstream.
     */
    @Test
    fun errorIsTerminalOperation() {
        val colors = Flowable.just("green", "blue", "red", "yellow")
                .map { color ->
                    if ("red" == color) {
                        throw RuntimeException("Encountered red")
                    }
                    color + "*"
                }
                .map { `val` -> `val` + "XXX" }

        subscribeWithLog(colors)
    }


    /**
     * The 'onErrorReturn' operator doesn't prevent the unsubscription from the 'colors'
     * but it does translate the exception for the downstream operators and the final Subscriber
     * receives it in the 'onNext()' instead in 'onError()'
     */
    @Test
    fun onErrorReturn() {
        val colors = Flowable.just("green", "blue", "red", "yellow")
                .map { color ->
                    if ("red" == color) {
                        throw RuntimeException("Encountered red")
                    }
                    color + "*"
                }
                .onErrorReturn { _ -> "-blank-" }
                .map { v -> v + "XXX" }

        subscribeWithLog(colors)
    }


    @Test
    fun onErrorReturnWithFlatMap() {
        //flatMap encounters an error when it subscribes to 'red' substreams and thus unsubscribe from
        // 'colors' stream and the remaining colors still are not longer emitted
        var colors = Flowable.just("green", "blue", "red", "white", "blue")
                .flatMap { color -> simulateRemoteOperation(color) }
                .onErrorReturn { _ -> "-blank-" } //onErrorReturn just has the effect of translating

        subscribeWithLog(colors)

        log.info("*****************")

        //bellow onErrorReturn() is applied to the flatMap substream and thus translates the exception to
        //a value and so flatMap continues on with the other colors after red
        colors = Flowable.just("green", "blue", "red", "white", "blue")
                .flatMap { color ->
                    simulateRemoteOperation(color)
                            .onErrorReturn { throwable -> "-blank-" }
                }

        subscribeWithLog(colors)
    }


    /**
     * onErrorResumeNext() returns a stream instead of an exception and subscribes to that stream instead,
     * useful for example to invoke a fallback method that returns also a stream
     */
    @Test
    fun onErrorResumeNext() {
        val colors = Flowable.just("green", "blue", "red", "white", "blue")
                .flatMap<String> { color ->
                    simulateRemoteOperation(color)
                            .onErrorResumeNext { th: Throwable ->
                                if (th is IllegalArgumentException) {
                                    Flowable.error<String>(RuntimeException("Fatal, wrong arguments"))
                                } else {
                                    fallbackRemoteOperation()
                                }
                            }
                }

        subscribeWithLog(colors)
    }

    private fun fallbackRemoteOperation(): Flowable<String> {
        return Flowable.just("blank")
    }


    /**
     * Retry Logic ****************
     */

    /**
     * timeout operator raises exception when there are no events incoming before it's predecessor in the specified
     * time limit
     *
     *
     * retry() resubscribes in case of exception to the Observable
     */
    @Test
    fun timeoutWithRetry() {
        val colors = Flowable.just("red", "blue", "green", "yellow")
                .concatMap { color ->
                    delayedByLengthEmitter(TimeUnit.SECONDS, color)
                            .timeout(6, TimeUnit.SECONDS)
                            .retry(2)
                            .onErrorResumeNext(Flowable.just("blank"))
                }

        subscribeWithLog(colors)
        //there is also
    }

    /**
     * When you want to retry based on the number considering the thrown exception type
     */
    @Test
    fun retryBasedOnAttemptsAndExceptionType() {
        var colors = Flowable.just("blue", "red", "black", "yellow")

        colors = colors
                .flatMap { colorName ->
                    simulateRemoteOperation(colorName, 2)
                            .retry { retryAttempt, exception ->
                                if (exception is IllegalArgumentException) {
                                    log.error("{} encountered non retry exception ", colorName)
                                    return@retry false
                                }
                                log.info("Retry attempt {} for {}", retryAttempt, colorName)
                                retryAttempt <= 3
                            }
                            .onErrorResumeNext(Flowable.just("generic color"))
                }

        subscribeWithLog(colors)
    }

    /**
     * A more complex retry logic like implementing a backoff strategy in case of exception
     * This can be obtained with retryWhen(exceptionObservable -> Observable)
     *
     *
     * retryWhen resubscribes when an event from an Observable is emitted. It receives as parameter an exception stream
     *
     *
     * we zip the exceptionsStream with a .range() stream to obtain the number of retries,
     * however we want to wait a little before retrying so in the zip function we return a delayed event - .timer()
     *
     *
     * The delay also needs to be subscribed to be effected so we also need flatMap
     */
    @Test
    fun retryWhenUsedForRetryWithBackoff() {
        var colors = Flowable.just("blue", "green", "red", "black", "yellow")

        colors = colors.flatMap { colorName ->
            simulateRemoteOperation(colorName, 3)
                    .retryWhen { exceptionStream ->
                        exceptionStream
                                .zipWith<Int, Flowable<Long>>(Flowable.range(1, 3), BiFunction { exc, attempts ->
                                    //don't retry for IllegalArgumentException
                                    if (exc is IllegalArgumentException) {
                                        return@BiFunction Flowable.error(exc)
                                    }

                                    if (attempts < 3) {
                                        log.info("Attempt {}, waiting before retry", attempts)
                                        return@BiFunction Flowable.timer((2 * attempts).toLong(), TimeUnit.SECONDS)
                                    }
                                    Flowable.error(exc)
                                })
                                .flatMap({ v -> v })
                    }
                    .onErrorResumeNext(Flowable.just("generic color"))
        }

        subscribeWithLog(colors)
    }

    /**
     * repeatWhen is identical to retryWhen only it responds to 'onCompleted' instead of 'onError'
     */
    @Test
    fun testRepeatWhen() {
        var remoteOperation = Flowable.defer {
            val random = Random()
            Flowable.just(random.nextInt(10))
        }

        remoteOperation = remoteOperation.repeatWhen { completed ->
            completed
                    .delay(2, TimeUnit.SECONDS)
        }
                .take(10)
        subscribeWithLogOutputWaitingForComplete(remoteOperation)
    }

    private fun simulateRemoteOperation(color: String, workAfterAttempts: Int = Integer.MAX_VALUE): Flowable<String> {
        return Flowable.create({ subscriber ->
            val attemptsHolder = attemptsMap.computeIfAbsent(color) { _ -> AtomicInteger(0) }
            val attempts = attemptsHolder.incrementAndGet()

            if ("red" == color) {
                checkAndThrowException(color, attempts, workAfterAttempts,
                        RuntimeException("Color red raises exception"))
            }
            if ("black" == color) {
                checkAndThrowException(color, attempts, workAfterAttempts,
                        IllegalArgumentException("Black is not a color"))
            }

            val value = "**$color**"

            log.info("Emitting {}", value)
            subscriber.onNext(value)
            subscriber.onComplete()
        }, BackpressureStrategy.BUFFER)
    }

    private fun checkAndThrowException(color: String, attempts: Int, workAfterAttempts: Int, exception: Exception) {
        if (attempts < workAfterAttempts) {
            log.info("Emitting {} for {}", exception.javaClass, color)
            throw IllegalArgumentException("Black is not a color")
        } else {
            log.info("After attempt {} we don't throw exception", attempts)
        }
    }

    companion object {

        private val attemptsMap = ConcurrentHashMap<String, AtomicInteger>()
    }
}
