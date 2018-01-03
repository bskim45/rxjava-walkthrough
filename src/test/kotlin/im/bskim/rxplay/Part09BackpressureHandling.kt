package ktplay

import io.reactivex.BackpressureOverflowStrategy
import io.reactivex.BackpressureStrategy
import io.reactivex.Flowable
import io.reactivex.functions.BiFunction
import io.reactivex.schedulers.Schedulers
import io.reactivex.subjects.PublishSubject
import ktplay.util.Helpers
import org.junit.Test
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Backpressure is related to preventing overloading the subscriber with too many events.
 * It can be the case of a slow consumer that cannot keep up with the producer.
 * Backpressure relates to a feedback mechanism through which the subscriber can signal
 * to the producer how much data it can consume.
 *
 * However the producer must be 'backpressure-aware' in order to know how to throttle back.
 *
 * If the producer is not 'backpressure-aware', in order to prevent an OutOfMemory due to an unbounded increase of events,
 * we still can define a BackpressureStrategy to specify how we should deal with piling events.
 * If we should buffer(BackpressureStrategy.BUFFER) or drop(BackpressureStrategy.DROP, BackpressureStrategy.LATEST)
 * incoming events.
 *
 * @author sbalamaci
 */
class Part09BackpressureHandling : BaseTestObservables() {


    @Test
    fun customBackpressureAwareFlux() {
        val flux = CustomRangeFlowable(5, 10)

        flux.subscribe(object : Subscriber<Int> {

            private var subscription: Subscription? = null
            private var backlogItems: Int = 0

            private val BATCH = 2
            private val INITIAL_REQ = 5

            override fun onSubscribe(subscription: Subscription) {
                this.subscription = subscription
                backlogItems = INITIAL_REQ

                log.info("Initial request {}", backlogItems)
                subscription.request(backlogItems.toLong())
            }

            override fun onNext(`val`: Int?) {
                log.info("Subscriber received {}", `val`)
                backlogItems--

                if (backlogItems == 0) {
                    backlogItems = BATCH
                    subscription!!.request(BATCH.toLong())
                }
            }

            override fun onError(throwable: Throwable) {
                log.info("Subscriber encountered error")
            }

            override fun onComplete() {
                log.info("Subscriber completed")
            }
        })
    }

    /**
     * We use BackpressureStrategy.DROP in create() to handle events that are emitted outside
     * the request amount from the downstream subscriber.
     *
     * We see that events that reach the subscriber are those only 3 requested by the
     * observeOn operator, the events produced outside of the requested amount
     * are discarded (BackpressureStrategy.DROP).
     *
     * observeOn has a default request size from upstream of 128(system parameter `rx2.buffer-size`)
     *
     */
    @Test
    fun createFlowableWithBackpressureStrategy() {
        val backpressureStrategy = BackpressureStrategy.DROP//              BackpressureStrategy.BUFFER
        //              BackpressureStrategy.LATEST
        //              BackpressureStrategy.ERROR

        var flowable = createFlowable(5, backpressureStrategy)

        //we need to switch threads to not run the producer in the same thread as the subscriber(which waits some time
        // to simulate a slow subscriber)
        flowable = flowable
                .observeOn(Schedulers.io(), false, 3)

        subscribeWithSlowSubscriberAndWait(flowable)
    }


    /**
     * There are operators for specifying backpressure strategy anywhere in the operators chain,
     * not just at Flowable.create().
     * - onBackpressureBuffer
     * - onBackpressureDrop
     * - onBackpressureLatest
     * These operators request from upstream the Long.MAX_VALUE(unbounded amount) and then they buffer(onBackpressureBuffer)
     * the events for downstream and send the events as requested.
     *
     * In the example we specify a buffering strategy in the example, however since the buffer is not very large,
     * we still get an exception after the 8th value - 3(requested) + 5(buffer)
     *
     * We create the Flowable with BackpressureStrategy.MISSING saying we don't care about backpressure
     * but let one of the onBackpressureXXX operators handle it.
     *
     */
    @Test
    fun bufferingBackpressureOperator() {
        var flowable = createFlowable(10, BackpressureStrategy.MISSING)
                .onBackpressureBuffer(5) { log.info("Buffer has overflown") }

        flowable = flowable
                .observeOn(Schedulers.io(), false, 3)

        subscribeWithSlowSubscriberAndWait(flowable)
    }

    /**
     * We can opt for a variant of the onBackpressureBuffer, to drop events that do not fit
     * inside the buffer
     */
    @Test
    fun bufferingThenDroppingEvents() {
        var flowable = createFlowable(10, BackpressureStrategy.MISSING)
                .onBackpressureBuffer(5, { log.info("Buffer has overflown") },
                        BackpressureOverflowStrategy.DROP_OLDEST)

        //we need to switch threads to not run the producer in the same thread as the subscriber(which waits some time
        // to simulate a slow subscriber)
        flowable = flowable
                .observeOn(Schedulers.io(), false, 3)

        subscribeWithSlowSubscriberAndWait(flowable)
    }


    /**
     * Not only a slow subscriber triggers backpressure, but also a slow operator
     * that slows down the handling of events and new request calls for new items
     */
    @Test
    fun throwingBackpressureNotSupportedSlowOperator() {
        val flowable = createFlowable(10, BackpressureStrategy.MISSING)
                .onBackpressureDrop { `val` -> log.info("Dropping {}", `val`) }
                .observeOn(Schedulers.io(), false, 3)
                .map { `val` ->
                    Helpers.sleepMillis(50)
                    "*$`val`*"
                }

        subscribeWithLogOutputWaitingForComplete(flowable) //notice it's not the slowSubscribe method used
    }

    /**
     * Backpressure operators can be added whenever necessary and it's not limited to
     * cold publishers and we can use them on hot publishers also
     */
    @Test
    fun backpressureWithHotPublisher() {
        val latch = CountDownLatch(1)

        val subject = PublishSubject.create<Int>()

        var flowable = subject
                .toFlowable(BackpressureStrategy.MISSING)
                .onBackpressureDrop { `val` -> log.info("Dropped {}", `val`) }

        flowable = flowable.observeOn(Schedulers.io(), false, 3)

        subscribeWithSlowSubscriber(flowable, latch)

        for (i in 1..10) {
            log.info("Emitting {}", i)
            subject.onNext(i)
        }
        subject.onComplete()

        Helpers.wait(latch)
    }

    /**
     * Chaining together multiple onBackpressureXXX operators doesn't actually make sense
     * Using
     * .onBackpressureBuffer(5)
     * .onBackpressureDrop((val) -> log.info("Dropping {}", val))
     * is not behaving as maybe expected - buffer 5 values, and then dropping overflowing events-.
     *
     * Because onBackpressureDrop subscribes to the previous onBackpressureBuffer operator
     * signaling its requesting Long.MAX_VALUE(unbounded amount) from it, the onBackpressureBuffer will never feel
     * its subscriber is overwhelmed and never "trigger" meaning that the last onBackpressureXXX operator overrides
     * the previous one.
     *
     * Of course for implementing an event dropping strategy after a full buffer, there is the special overrided
     * version of onBackpressureBuffer that takes a BackpressureOverflowStrategy.
     */
    @Test
    fun cascadingOnBackpressureXXXOperators() {
        val flowable = createFlowable(10, BackpressureStrategy.MISSING)
                .onBackpressureBuffer(5)
                .onBackpressureDrop { `val` -> log.info("Dropping {}", `val`) }
                .observeOn(Schedulers.io(), false, 3)

        subscribeWithSlowSubscriberAndWait(flowable)
    }


    /**
     * Zipping a slow stream with a faster one also can cause a backpressure problem
     */
    @Test
    fun zipOperatorHasALimit() {
        val fast = createFlowable(200, BackpressureStrategy.MISSING)
        val slowStream = Flowable.interval(100, TimeUnit.MILLISECONDS)

        val observable = Flowable.zip<Int, Long, String>(fast, slowStream,
                BiFunction { val1, val2 -> "$val1 $val2" })

        subscribeWithSlowSubscriberAndWait(observable)
    }

    @Test
    fun backpressureAwareObservable() {
        var flowable = Flowable.range(0, 10)

        flowable = flowable
                .observeOn(Schedulers.io(), false, 3)

        subscribeWithSlowSubscriberAndWait(flowable)
    }


    private fun createFlowable(items: Int,
                               backpressureStrategy: BackpressureStrategy): Flowable<Int> {
        return Flowable.create({ subscriber ->
            log.info("Started emitting")

            for (i in 0 until items) {
                if (subscriber.isCancelled) {
                    return@create
                }

                log.info("Emitting {}", i)
                subscriber.onNext(i)
            }

            subscriber.onComplete()
        }, backpressureStrategy)
    }


    private fun <T> subscribeWithSlowSubscriberAndWait(flowable: Flowable<T>) {
        val latch = CountDownLatch(1)
        flowable.subscribe(logNextAndSlowByMillis(50), logError(latch), logComplete(latch))

        Helpers.wait(latch)
    }

    private fun <T> subscribeWithSlowSubscriber(flowable: Flowable<T>, latch: CountDownLatch) {
        flowable.subscribe(logNextAndSlowByMillis(50), logError(latch), logComplete(latch))
    }

    private inner class CustomRangeFlowable internal constructor(private val startFrom: Int, private val count: Int) : Flowable<Int>() {

        public override fun subscribeActual(subscriber: Subscriber<in Int>) {
            subscriber.onSubscribe(CustomRangeSubscription(startFrom, count, subscriber))
        }

        internal inner class CustomRangeSubscription(private val startFrom: Int, private val count: Int, private val actualSubscriber: Subscriber<in Int>) : Subscription {

            @Volatile
            var cancelled: Boolean = false
            var completed = false
            private var currentCount: Int = 0

            override fun request(items: Long) {
                log.info("Downstream requests {} items", items)
                for (i in 0 until items) {
                    if (cancelled || completed) {
                        return
                    }

                    if (currentCount == count) {
                        completed = true
                        if (cancelled) {
                            return
                        }

                        actualSubscriber.onComplete()
                        return
                    }

                    val emitVal = startFrom + currentCount
                    currentCount++
                    actualSubscriber.onNext(emitVal)
                }
            }

            override fun cancel() {
                cancelled = true
            }
        }
    }


}
