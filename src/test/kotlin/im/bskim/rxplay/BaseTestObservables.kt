package ktplay

import io.reactivex.BackpressureStrategy
import io.reactivex.Flowable
import io.reactivex.Observable
import io.reactivex.Single
import io.reactivex.functions.BiFunction
import ktplay.util.Helpers
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * @author sbalamaci
 */
abstract class BaseTestObservables {
    val log: Logger by lazy { LoggerFactory.getLogger(BaseTestObservables::class.java) }

    fun simpleFlowable(): Flowable<Int> {
        return Flowable.create({ subscriber ->
            log.info("Started emitting")

            log.info("Emitting 1st")
            subscriber.onNext(1)

            log.info("Emitting 2nd")
            subscriber.onNext(2)

            subscriber.onComplete()
        }, BackpressureStrategy.BUFFER)
    }

    fun <T> subscribeWithLog(flowable: Flowable<T>) {
        flowable.subscribe(
                logNext(),
                logError(),
                logComplete()
        )
    }

    fun <T> subscribeWithLog(observable: Observable<T>) {
        observable.subscribe(
                logNext(),
                logError(),
                logComplete()
        )
    }

    fun <T> subscribeWithLogOutputWaitingForComplete(observable: Observable<T>) {
        val latch = CountDownLatch(1)

        observable.subscribe(
                logNext(),
                logError(latch),
                logComplete(latch)
        )

        Helpers.wait(latch)
    }

    fun <T> subscribeWithLog(single: Single<T>) {
        single.subscribe({ v -> log.info("Subscriber received: {}", v) },
                logError()
        )
    }

    fun <T> subscribeWithLogOutputWaitingForComplete(flowable: Flowable<T>) {
        val latch = CountDownLatch(1)

        flowable.subscribe(
                logNext(),
                logError(latch),
                logComplete(latch)
        )

        Helpers.wait(latch)
    }

    fun <T> subscribeWithLogOutputWaitingForComplete(single: Single<T>) {
        val latch = CountDownLatch(1)

        single.subscribe({ v: T ->
                    log.info("Subscriber received: {} and completed", v.toString())
                    latch.countDown()
                },
                logError(latch)
        )

        Helpers.wait(latch)
    }

    fun <T> periodicEmitter(t1: T, t2: T, t3: T, interval: Int, unit: TimeUnit): Flowable<T> {
        return periodicEmitter(t1, t2, t3, interval, unit, interval)
    }

    fun <T> periodicEmitter(t1: T, t2: T, t3: T, interval: Int,
                            unit: TimeUnit, initialDelay: Int): Flowable<T> {
        val itemsStream = Flowable.just(t1, t2, t3)
        val timer = Flowable.interval(initialDelay.toLong(), interval.toLong(), unit)

        return Flowable.zip(itemsStream, timer, BiFunction { key, _ -> key })
    }

    fun <T> periodicEmitter(items: Array<T>, interval: Int,
                            unit: TimeUnit, initialDelay: Int): Observable<T> {
        val itemsStream = Observable.fromArray(*items)
        val timer = Observable.interval(initialDelay.toLong(), interval.toLong(), unit)

        return Observable.zip(itemsStream, timer, BiFunction { key, _ -> key })
    }

    fun <T> periodicEmitter(items: Array<T>, interval: Int,
                            unit: TimeUnit): Observable<T> {
        return periodicEmitter(items, interval, unit)
    }

    fun delayedByLengthEmitter(unit: TimeUnit, vararg items: String): Flowable<String> {
        val itemsStream = Flowable.fromArray(*items)

        return itemsStream.concatMap { item ->
            Flowable.just(item)
                    .doOnNext { `val` -> log.info("Received {} delaying for {} ", `val`, `val`.length) }
                    .delay(item.length.toLong(), unit)
        }
    }

    fun <T> logNext(v: T) {
        log.info("Subscriber received: {}", v)
    }

    fun <T> logNext() = { v: T -> log.info("Subscriber received: {}", v) }

    fun <T> logNextAndSlowByMillis(millis: Int) = { v: T ->
            log.info("Subscriber received: {}", v)
            Helpers.sleepMillis(millis)
    }

    fun logError() = { err: Throwable ->
        log.error("Subscriber received error '{}'", err.message)
    }

    fun logError(latch: CountDownLatch) = { err: Throwable ->
            log.error("Subscriber received error '{}'", err.message)
            latch.countDown()
    }

    fun logComplete() = { log.info("Subscriber got Completed event") }

    fun logComplete(latch: CountDownLatch) = {
        log.info("Subscriber got Completed event")
        latch.countDown()
    }
}


