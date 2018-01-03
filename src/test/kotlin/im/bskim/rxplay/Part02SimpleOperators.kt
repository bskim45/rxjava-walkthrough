package ktplay

import io.reactivex.Flowable
import ktplay.util.Helpers
import org.junit.Test
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * @author sbalamaci
 */
class Part02SimpleOperators : BaseTestObservables() {

    /**
     * Delay operator - the Thread.sleep of the reactive world, it's pausing for a particular increment of time
     * before emitting the whole range events which are thus shifted by the specified time amount.
     *
     * The delay operator uses a Scheduler {@see Part06Schedulers} by default, which actually means it's
     * running the operators and the subscribe operations on a different thread, which means the test method
     * will terminate before we see the text from the log. That is why we use the CountDownLatch waiting for the
     * completion of the stream.
     *
     */
    @Test
    fun delayOperator() {
        log.info("Starting")

        val latch = CountDownLatch(1)
        Flowable.range(0, 2)
                .doOnNext { `val` -> log.info("Emitted {}", `val`) }
                .delay(5, TimeUnit.SECONDS)
                .subscribe(
                        { tick -> log.info("Tick {}", tick) },
                        { log.info("Error emitted") }
                ) {
                    log.info("Completed")
                    latch.countDown()
                }

        Helpers.wait(latch)
    }

    /**
     * Timer operator waits for a specific amount of time before it emits an event and then completes
     */
    @Test
    fun timerOperator() {
        log.info("Starting")
        val flowable = Flowable.timer(5, TimeUnit.SECONDS)
        subscribeWithLogOutputWaitingForComplete(flowable)
    }


    @Test
    fun delayOperatorWithVariableDelay() {
        log.info("Starting")
        val flowable = Flowable.range(0, 5)
                .doOnNext { `val` -> log.info("Emitted {}", `val`) }
                .delay { `val` -> Flowable.timer((`val` * 2).toLong(), TimeUnit.SECONDS) }
        subscribeWithLogOutputWaitingForComplete(flowable)
    }

    /**
     * Periodically emits a number starting from 0 and then increasing the value on each emission
     */
    @Test
    fun intervalOperator() {
        log.info("Starting")
        val flowable = Flowable.interval(1, TimeUnit.SECONDS)
                .take(5)

        subscribeWithLogOutputWaitingForComplete(flowable)
    }

    /**
     * scan operator - takes an initial value and a function(accumulator, currentValue). It goes through the events
     * sequence and combines the current event value with the previous result(accumulator) emitting downstream the
     * the function's result for each event(the initial value is used for the first event).
     */
    @Test
    fun scanOperator() {
        val numbers = Flowable.just(3, 5, -2, 9)
                .scan(0) { totalSoFar, currentValue ->
                    log.info("totalSoFar={}, emitted={}", totalSoFar, currentValue)
                    totalSoFar + currentValue
                }

        subscribeWithLog(numbers)
    }

    /**
     * reduce operator acts like the scan operator but it only passes downstream the final result
     * (doesn't pass the intermediate results downstream) so the subscriber receives just one event
     */
    @Test
    fun reduceOperator() {
        val numbers = Flowable.just(3, 5, -2, 9)
                .reduce(0) { totalSoFar, `val` ->
                    log.info("totalSoFar={}, emitted={}", totalSoFar, `val`)
                    totalSoFar + `val`
                }
        subscribeWithLog(numbers)
    }

    /**
     * collect operator acts similar to the reduce() operator, but while the reduce() operator uses a reduce function
     * which returns a value, the collect() operator takes a container supplie and a function which doesn't return
     * anything(a consumer). The mutable container is passed for every event and thus you get a chance to modify it
     * in this collect consumer function
     */
    @Test
    fun collectOperator() {
        val numbers = Flowable.just(3, 5, -2, 9)
                .collect({ mutableListOf<Int>()}, { container, value ->
                    log.info("Adding {} to container", value)
                    container.add(value)
                    //notice we don't need to return anything
                })
        subscribeWithLog(numbers)
    }

    /**
     * repeat resubscribes to the observable after it receives onComplete
     */
    @Test
    fun repeat() {
        val random = Flowable.defer {
            val rand = Random()
            Flowable.just(rand.nextInt(20))
        }
                .repeat(5)

        subscribeWithLogOutputWaitingForComplete(random)
    }

}
