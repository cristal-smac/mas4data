package utils.mailboxes

import akka.actor.{ActorRef, ActorSystem}
import akka.dispatch.{Envelope, MailboxType, MessageQueue, ProducesMessageQueue}
import com.typesafe.config.Config
import mapreduce.adaptive.Manager
import utils.config.ConfigurationBuilder

import scala.collection.mutable

// Trait to identify the manager message queue
trait ManagerWithWorkerPriorityMessageQueueSemantics

/** Companion object of the ManagerMailBox */
object ManagerMailBoxWithWorkerPriority {

  /** Manager message queue
    *
    * Specific message queue for the manager mail box.
    *
    * @param frequency frequency on which the queue provide a outdated
    *                  InformContribution message
    */
  class ManagerWithWorkerPriorityMessageQueue(frequency: Int)
    extends MessageQueue with ManagerWithWorkerPriorityMessageQueueSemantics {

    // Internal queue
    private final val queue = new mutable.Queue[Envelope]()

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
    override def enqueue(receiver: ActorRef, handle: Envelope): Unit =
      handle match {
        // If the message is an InformContribution
        case Envelope(Manager.InformContribution(acq, _), _) =>
          val (inQueue, countdown) = this.acqWorkloads(acq)

          // The data structure is updated
          this.acqWorkloads.update(acq, (inQueue + 1, countdown))
          this.queue.enqueue(handle)

        case Envelope(Manager.WorkerDone, _) => handle +=: this.queue

        case _ => this.queue.enqueue(handle)
      }

    // Dequeue a message
    override def dequeue(): Envelope = try {
      val envelope = this.queue.dequeue()

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
    } catch {
      case _: Exception => null
    }

    // Get the number of messages in the queue
    override def numberOfMessages: Int = this.queue.size

    // Return true iff the queue still has messages in
    override def hasMessages: Boolean = this.queue.nonEmpty

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
class ManagerMailBoxWithWorkerPriority extends MailboxType
  with ProducesMessageQueue[
    ManagerMailBoxWithWorkerPriority.ManagerWithWorkerPriorityMessageQueue
  ] {

  import ManagerMailBoxWithWorkerPriority._

  def this(settings: ActorSystem.Settings, config: Config) = {
    this()
  }

  final override def create(
    owner: Option[ActorRef],
    system: Option[ActorSystem]
  ): MessageQueue = new ManagerWithWorkerPriorityMessageQueue(
    ConfigurationBuilder.config.informContributionFrequency
  )

}
