package utils.mailboxes

import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.{ActorRef, ActorSystem}
import akka.dispatch.{Envelope, MailboxType, MessageQueue, ProducesMessageQueue}
import com.typesafe.config.Config
import mapreduce.adaptive.Manager
import utils.config.ConfigurationBuilder

import scala.collection.mutable

// Trait to identify the manager message queue
trait ManagerMessageQueueSemantics

/** Companion object of the ManagerMailBox */
object ManagerMailBox {

  /** Manager message queue
    *
    * Specific message queue for the manager mail box.
    *
    * @param frequency frequency on which the queue provide a outdated
    *                  InformContribution message
    */
  class ManagerMessageQueue(frequency: Int) extends MessageQueue
    with ManagerMessageQueueSemantics {

    // Internal queue
    private final val queue = new ConcurrentLinkedQueue[Envelope]()

    /* Data structure to register incoming InformContribution messages.
     * Each InformContribution is registered for each acquaintance.
     *
     * The first int represents the number of InformContribution from this
     * acquaintance there are in the queue.
     *
     * The second int is a countdown. Each <countdown>th outdated
     * InformContribution is still sent.
     */
    private val acqWorkloads =
      mutable.Map[ActorRef, (Int, Int)]() withDefaultValue (0, this.frequency)

    // Enqueue a message
    override def enqueue(receiver: ActorRef, handle: Envelope): Unit = {
      handle match {
        // If the message is an InformContribution
        case Envelope(Manager.InformContribution(acq, _), _) =>
          val (inQueue, countdown) = this.acqWorkloads(acq)

          // The data structure is updated
          this.acqWorkloads.update(acq, (inQueue + 1, countdown))

        case _ =>
      }

      // Whatever the message is, it is enqueue
      this.queue.offer(handle)
    }

    // Dequeue a message
    override def dequeue(): Envelope = {
      // The first message of the queue
      val envelope = this.queue.poll()

      envelope match {
        // If this message is an InformContribution...
        case Envelope(Manager.InformContribution(acq, _), _) =>
          val (inQueue, countdown) = this.acqWorkloads(acq)
          val (newInQueue, newCountdown) = (inQueue - 1, countdown - 1)

          // ... and the message is up to date (or the countdown is over)
          if (newInQueue == 0 || newCountdown == 0) {
            // The data structure is updated
            this.acqWorkloads.update(acq, (newInQueue, this.frequency))
            // And the message is sent
            envelope
          }
          // ... but is outdated, it is skipped
          else {
            // The data structure is updated
            this.acqWorkloads.update(acq, (newInQueue, newCountdown))
            this.dequeue()
          }

        // If the message is another one, it is sent
        case _ => envelope
      }
    }

    // Get the number of messages in the queue
    override def numberOfMessages: Int = this.queue.size

    // Return true iff the queue still has messages in
    override def hasMessages: Boolean = !this.queue.isEmpty

    // Perform a clean up of the queue
    override def cleanUp(owner: ActorRef, deadLetters: MessageQueue): Unit = {
      while (this.hasMessages) {
        deadLetters.enqueue(owner, this.dequeue())
      }
    }

  }

}

/** ManagerMailBox class
  *
  * Mail box which uses a manager message queue.
  */
class ManagerMailBox extends MailboxType
  with ProducesMessageQueue[ManagerMailBox.ManagerMessageQueue] {

  import ManagerMailBox._

  def this(settings: ActorSystem.Settings, config: Config) = {
    this()
  }

  final override def create(
    owner: Option[ActorRef],
    system: Option[ActorSystem]
  ): MessageQueue = new ManagerMessageQueue(
    ConfigurationBuilder.config.informContributionFrequency
  )

}
