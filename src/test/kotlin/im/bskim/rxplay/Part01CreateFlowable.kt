package ktplay

import io.reactivex.BackpressureStrategy
import io.reactivex.Flowable
import io.reactivex.Observable
import io.reactivex.Single
import ktplay.util.Helpers
import org.junit.Test
import java.util.*
import java.util.concurrent.CompletableFuture

/**
 *
 *
 * @author sbalamaci
 */
class Part01CreateFlowable : BaseTestObservables() {
    @Test
    fun just() {
        val flowable = Flowable.just(1, 5, 10)

        flowable.subscribe { v -> log.info("Subscriber received: {}", v) }
    }

    @Test
    fun range() {
        val flowable = Flowable.range(1, 10)

        flowable.subscribe { v -> log.info("Subscriber received: {}", v) }
    }

    @Test
    fun fromArray() {
        val flowable = Flowable.fromArray("red", "green", "blue", "black")

        flowable.subscribe { log.info("Subscriber received: {}") }
    }

    @Test
    fun fromIterable() {
        val flowable = Flowable.fromIterable(Arrays.asList("red", "green", "blue"))

        flowable.subscribe { log.info("Subscriber received: {}") }
    }

    /**
     * We can also create a stream from Future, making easier to switch from legacy code to reactive
     */
    @Test
    fun fromFuture() {
        val completableFuture = CompletableFuture.supplyAsync {
            //starts a background thread the ForkJoin common pool
            Helpers.sleepMillis(100)
            "red"
        }

        val single = Single.fromFuture(completableFuture)
        single.subscribe { v -> log.info("Stream completed successfully : {}", v) }
    }


    /**
     * Using Flowable.create to handle the actual emissions of events with the events like onNext, onComplete, onError
     *
     *
     * When subscribing to the Flowable / Observable with flowable.subscribe(), the lambda code inside create() gets executed.
     * Flowable.subscribe can take 3 handlers for each type of event - onNext, onError and onComplete
     *
     *
     * When using Observable.create you need to be aware of **Backpressure** and that Observables based on 'create' method
     * are not Backpressure aware {@see Part09BackpressureHandling}.
     */
    @Test
    fun createSimpleObservable() {
        val flowable = Flowable.create<Int>({ subscriber ->
            log.info("Started emitting")

            log.info("Emitting 1st")
            subscriber.onNext(1)

            log.info("Emitting 2nd")
            subscriber.onNext(2)

            subscriber.onComplete()
        }, BackpressureStrategy.BUFFER)

        log.info("Subscribing")
        val subscription = flowable.subscribe(
                { v -> log.info("Subscriber received: {}", v) },
                { err -> log.error("Subscriber received error", err) },
                { log.info("Subscriber got Completed event") })
    }


    /**
     * Observable emits an Error event which is a terminal operation and the subscriber is no longer executing
     * it's onNext callback. We're actually breaking the the Observable contract that we're still emitting events
     * after onComplete or onError have fired.
     */
    @Test
    fun createSimpleObservableThatEmitsError() {
        val observable = Observable.create<Int> { subscriber ->
            log.info("Started emitting")

            log.info("Emitting 1st")
            subscriber.onNext(1)

            subscriber.onError(RuntimeException("Test exception"))

            log.info("Emitting 2nd")
            subscriber.onNext(2)
        }

        val disposable = observable.subscribe(
                { v -> log.info("Subscriber received: {}", v) },
                { err -> log.error("Subscriber received error", err) }
        ) { log.info("Subscriber got Completed event") }
    }

    /**
     * Observables are lazy, meaning that the code inside create() doesn't get executed without subscribing to the Observable
     * So even if we sleep for a long time inside create() method(to simulate a costly operation),
     * without subscribing to this Observable the code is not executed and the method returns immediately.
     */
    @Test
    fun flowablesAreLazy() {
        val flowable = Observable.create<Int> { emitter ->
            log.info("Started emitting but sleeping for 5 secs") //this is not executed
            Helpers.sleepMillis(5000)
            emitter.onNext(1)
        }
        log.info("Finished")
    }

    /**
     * When subscribing to an Observable, the create() method gets executed for each subscription
     * this means that the events inside create are re-emitted to each subscriber. So every subscriber will get the
     * same events and will not lose any events.
     */
    @Test
    fun multipleSubscriptionsToSameObservable() {
        val flowable = Observable.create<Int> { subscriber ->
            log.info("Started emitting")

            log.info("Emitting 1st event")
            subscriber.onNext(1)

            log.info("Emitting 2nd event")
            subscriber.onNext(2)

            subscriber.onComplete()
        }

        log.info("Subscribing 1st subscriber")
        flowable.subscribe { v -> log.info("First Subscriber received: {}", v) }

        log.info("=======================")

        log.info("Subscribing 2nd subscriber")
        flowable.subscribe { v -> log.info("Second Subscriber received: {}", v) }
    }

    /**
     * Inside the create() method, we can check is there are still active subscribers to our Observable.
     * It's a way to prevent to do extra work(like for ex. querying a datasource for entries) if no one is listening
     * In the following example we'd expect to have an infinite stream, but because we stop if there are no active
     * subscribers we stop producing events.
     * The take() operator unsubscribes from the Observable after it's received the specified amount of events
     * while in the same time calling onComplete() downstream.
     */
    @Test
    fun showUnsubscribeObservable() {
        val observable = Observable.create<Int> { subscriber ->
            var i = 1
            while (true) {
                if (subscriber.isDisposed) {
                    break
                }

                subscriber.onNext(i++)
            }
            //subscriber.onCompleted(); too late to emit Complete event since subscriber already unsubscribed

            subscriber.setCancellable { log.info("Subscription canceled") }
        }

        observable
                .take(5)
                .map { v -> "*$v*" }
                .subscribe(
                        { v -> log.info("Subscriber received: {}", v) },
                        { err -> log.error("Subscriber received error", err) },
                        { log.info("Subscriber got Completed event") } //The Complete event is triggered by 'take()' operator
                )
    }


    /**
     * .defer acts as a factory of Flowables, just when subscribed it actually invokes the logic to create the
     * Flowable to be emitted.
     * It's an easy way to switch from a blocking method to a reactive Single/Flowable.
     * Simply using Flowable.just(blockingOp()) would still block, as Java needs to resolve the parameter when invoking
     * Flux.just(param) method, so blockingOp() method would still be invoked(and block).
     *
     * The solution is to wrap the blockingOp() method inside a lambda that gets passed to .defer(() -> blockingOp())
     *
     */
    @Test
    fun deferCreateObservable() {
        log.info("Starting blocking Flowable")
        val flowableBlocked = Flowable.just(blockingOperation())
        log.info("After blocking Flowable")

        log.info("Starting defered op")
        val stream = Flowable.defer { Flowable.just(blockingOperation()) }
        log.info("After defered op")

        log.info("Sleeping a little before subscribing and executing the defered code")
        Helpers.sleepMillis(2000)

        log.info("Subscribing")
        subscribeWithLogOutputWaitingForComplete(stream)
    }

    private fun blockingOperation(): String {
        log.info("Blocking 1sec...")
        Helpers.sleepMillis(1000)
        log.info("Ended blocking")

        return "Hello"
    }

}
