package ktplay.util

import org.slf4j.LoggerFactory
import java.util.concurrent.CountDownLatch

/**
 * @author sbalamaci
 */
object Helpers {

    private val log = LoggerFactory.getLogger(Helpers::class.java)

    fun sleepMillis(millis: Int) {
        try {
            Thread.sleep(millis.toLong())
        } catch (e: InterruptedException) {
            log.error("Interrupted Thread")
            throw RuntimeException("Interrupted thread")
        }

    }

    fun wait(waitOn: CountDownLatch) {
        try {
            waitOn.await()
        } catch (e: InterruptedException) {
            log.error("Interrupted waiting on CountDownLatch")
            throw RuntimeException("Interrupted thread")
        }

    }

}
