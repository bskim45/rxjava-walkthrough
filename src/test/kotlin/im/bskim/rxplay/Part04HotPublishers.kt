package ktplay

import io.reactivex.Observable
import io.reactivex.observables.ConnectableObservable
import io.reactivex.subjects.PublishSubject
import io.reactivex.subjects.ReplaySubject
import io.reactivex.subjects.Subject
import ktplay.util.Helpers
import org.junit.Test

import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

/**
 * @author sbalamaci
 */
class Part04HotPublishers : BaseTestObservables() {

    /**
     * We've seen that with 'cold publishers', whenever a subscriber subscribes, each subscriber will get
     * it's version of emitted values independently
     *
     * Subjects keep reference to their subscribers and allow 'multicasting' to them.
     *
     * for (Disposable<T> s : subscribers.get()) {
     * s.onNext(t);
     * }
     *
     *
     * Subjects besides being traditional Observables you can use the same operators and subscribe to them,
     * are also an Observer, meaning you can invoke subject.onNext(value) from different parts in the code,
     * which means that you publish events which the Subject will pass on to their subscribers.
     *
     * Observable.create(subscriber -> {
     * subscriber.onNext(val);
     * })
     * .map(...)
     * .subscribe(...);
     *
     * With Subjects you can call onNext from different parts of the code:
     * Subject<Integer> subject = ReplaySubject.create()
     * .map(...);
     * .subscribe(); //
     *
     * ...
     * subject.onNext(val);
     * ...
     * subject.onNext(val2);
     *
     * ReplaySubject keeps a buffer of events that it 'replays' to each new subscriber, first he receives a batch of missed
     * and only later events in real-time.
     *
     * PublishSubject - doesn't keep a buffer but instead, meaning if another subscriber subscribes later, it's going to loose events
    </Integer></T> */

    @Test
    fun replaySubject() {
        val subject = ReplaySubject.createWithSize<Int>(50)

        //        Runnable pushAction = pushEventsToSubjectAction(subject, 10);
        //        periodicEventEmitter(pushAction, 500, TimeUnit.MILLISECONDS);

        pushToSubject(subject, 0)
        pushToSubject(subject, 1)

        val latch = CountDownLatch(2)
        log.info("Subscribing 1st")
        subject.subscribe({ `val` -> log.info("Subscriber1 received {}", `val`) }, logError(), logComplete(latch))

        pushToSubject(subject, 2)

        log.info("Subscribing 2nd")
        subject.subscribe({ `val` -> log.info("Subscriber2 received {}", `val`) }, logError(), logComplete(latch))
        pushToSubject(subject, 3)

        subject.onComplete()

        Helpers.wait(latch)
    }

    private fun pushToSubject(subject: Subject<Int>, `val`: Int) {
        log.info("Pushing {}", `val`)
        subject.onNext(`val`)
    }

    @Test
    fun publishSubject() {
        val subject = PublishSubject.create<Int>()

        Helpers.sleepMillis(1000)
        log.info("Subscribing 1st")

        val latch = CountDownLatch(2)
        subject
                .subscribe({ `val` -> log.info("Subscriber1 received {}", `val`) }, logError(), logComplete(latch))

        Helpers.sleepMillis(1000)
        log.info("Subscribing 2nd")
        subject.subscribe({ `val` -> log.info("Subscriber2 received {}", `val`) }, logError(), logComplete(latch))
        Helpers.wait(latch)
    }


    /**
     * Because reactive stream specs mandates that events should be ordered(cannot emit downstream two events simultaneously)
     * it means that it's illegal to call onNext,onComplete,onError from different threads.
     *
     * To make this easy there is the .toSerialized() operator that wraps the Subject inside a SerializedSubject
     * which basically just calls the onNext,.. methods of the wrapped Subject inside a synchronized block.
     */
    @Test
    fun callsToSubjectMethodsMustHappenOnSameThread() {
        val subject = PublishSubject.create<Int>()

        val latch = CountDownLatch(1)

        val serializedSubject = subject.toSerialized()
        subject.subscribe(logNext(), logError(), logComplete(latch))

        Thread({ serializedSubject.onNext(1) }, "thread1").start()
        Thread({ serializedSubject.onNext(2) }, "thread2").start()
        Thread({ serializedSubject.onComplete() }, "thread3").start()

        Helpers.wait(latch)
    }


    /**
     * ConnectableObservable is a special kind of Observable that when calling .subscribe()
     * it just keeps a reference to its subscribers, it only subscribes once the .connect() method is called
     */
    @Test
    fun sharingResourcesBetweenSubscriptions() {
        val connectableObservable = Observable.create<Int> { subscriber ->
            log.info("Inside create()")

            /* A JMS connection listener example
               Just an example of a costly operation that is better to be shared **/

            /* Connection connection = connectionFactory.createConnection();
              Session session = connection.createSession(true, AUTO_ACKNOWLEDGE);
              MessageConsumer consumer = session.createConsumer(orders);
              consumer.setMessageListener(subscriber::onNext); */

            subscriber.setCancellable { log.info("Subscription cancelled") }

            log.info("Emitting 1")
            subscriber.onNext(1)

            log.info("Emitting 2")
            subscriber.onNext(2)

            subscriber.onComplete()
        }.publish() //calling .publish makes an Observable a ConnectableObservable

        log.info("Before subscribing")
        val latch = CountDownLatch(2)

        /* calling .subscribe() bellow doesn't actually subscribe, but puts them in a list to actually subscribe
           when calling .connect() */
        connectableObservable
                .take(1)
                .subscribe({ `val` -> log.info("Subscriber1 received: {}", `val`) }, logError(), logComplete(latch))

        connectableObservable
                .subscribe({ `val` -> log.info("Subscriber2 received: {}", `val`) }, logError(), logComplete(latch))


        //we need to call .connect() to trigger the real subscription
        log.info("Now connecting to the ConnectableObservable")
        connectableObservable.connect()

        Helpers.wait(latch)
    }

    /**
     * We can get away with having to call ourselves .connect(), by using
     */
    @Test
    fun autoConnectingWithFirstSubscriber() {
        val connectableObservable = Observable.create<Int> { subscriber ->
            log.info("Inside create()")

            //simulating some listener that produces events after
            //connection is initialized
            val resourceConnectionHandler = object : ResourceConnectionHandler() {
                override fun onMessage(message: Int) {
                    log.info("Emitting {}", message)
                    subscriber.onNext(message)
                }
            }

            resourceConnectionHandler.openConnection()

            subscriber.setCancellable({ resourceConnectionHandler.disconnect() })
        }.publish()

        val observable = connectableObservable.autoConnect()

        val latch = CountDownLatch(2)
        observable
                .take(5)
                .subscribe({ `val` -> log.info("Subscriber1 received: {}", `val`) }, logError(), logComplete(latch))
        Helpers.sleepMillis(1000)

        observable
                .take(2)
                .subscribe({ `val` -> log.info("Subscriber2 received: {}", `val`) }, logError(), logComplete(latch))

        Helpers.wait(latch)
    }

    /**
     * Even the above .autoConnect() can be improved
     */
    @Test
    fun refCountTheConnectableObservableAutomaticSubscriptionOperator() {
        val connectableObservable = Observable.create<Int> { subscriber ->
            log.info("Inside create()")

            //simulating some listener that produces events after
            //connection is initialized
            val resourceConnectionHandler = object : ResourceConnectionHandler() {
                override fun onMessage(message: Int) {
                    log.info("Emitting {}", message)
                    subscriber.onNext(message)
                }
            }
            resourceConnectionHandler.openConnection()

            subscriber.setCancellable({ resourceConnectionHandler.disconnect() })
        }.publish()

        val observable = connectableObservable.refCount()
        //publish().refCount() equals share()

        var latch = CountDownLatch(2)
        observable
                .take(5)
                .subscribe({ `val` -> log.info("Subscriber1 received: {}", `val`) }, logError(), logComplete(latch))

        Helpers.sleepMillis(1000)

        log.info("Subscribing 2nd")
        //we're not seeing the code inside .create() re-executed
        observable
                .take(2)
                .subscribe({ `val` -> log.info("Subscriber2 received: {}", `val`) }, logError(), logComplete(latch))

        Helpers.wait(latch)

        //Previous Subscribers all unsubscribed, subscribing another will trigger the execution of the code
        //inside .create()
        latch = CountDownLatch(1)
        log.info("Subscribing 3rd")
        observable
                .take(1)
                .subscribe({ `val` -> log.info("Subscriber3 received: {}", `val`) }, logError(), logComplete(latch))
        Helpers.wait(latch)
    }


    private fun periodicEventEmitter(action: Runnable,
                                     period: Int, timeUnit: TimeUnit): ScheduledExecutorService {
        val scheduledExecutorService = Executors.newScheduledThreadPool(1)
        scheduledExecutorService.scheduleAtFixedRate(action, 0, period.toLong(), timeUnit)

        return scheduledExecutorService
    }


    private abstract inner class ResourceConnectionHandler {

        internal lateinit var scheduledExecutorService: ScheduledExecutorService

        private var counter: Int = 0

        fun openConnection() {
            log.info("**Opening connection")

            scheduledExecutorService = periodicEventEmitter(Runnable {
                counter++
                onMessage(counter)
            }, 500, TimeUnit.MILLISECONDS)
        }

        abstract fun onMessage(message: Int)

        fun disconnect() {
            log.info("**Shutting down connection")
            scheduledExecutorService.shutdown()
        }
    }

    /*    private Runnable pushEventsToSubjectAction(Subject<Integer> subject, int maxEvents) {
        return () -> {
            if(counter == maxEvents) {
                subject.onComplete();
                return;
            }

            counter ++;

            log.info("Emitted {}", counter);
            subject.onNext(counter);
        };
    }*/
}
