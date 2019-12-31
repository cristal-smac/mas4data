package mapreduce.time

import akka.actor.{ Actor, ReceiveTimeout }
import scala.concurrent.duration._
import scala.language.postfixOps

/** Companion object of the Timer class.
  *
  * Contain all the messages that a Timer agent could receive
  */
object Timer {

  /** Message Start
    *
    * Tell to the timer to begin the countdown.
    */
  object Start

  /** Message Cancel
    *
    * Tell to the timer to cancel the countdown.
    */
  object Cancel

  /** Message Timeout
    *
    * Tell to the timer creator that the countdown is over.
    */
  object Timeout

}

/** Timer actor which makes a countdown.
  *
  * @param duration duration of the timer in milliseconds
  */
class Timer(duration: Int) extends Actor with utils.debugs.Debug {

  import Timer._

  this.setDebug(false)

  def receive: Receive = this.active orElse this.handleUnexpected

  /** Active state of the timer. */
  def active: Receive = {

    case Start if sender == context.parent =>
      debugReceive("Start", sender, "active")
      context.setReceiveTimeout(Duration(this.duration, MILLISECONDS))

    case ReceiveTimeout =>
      debugReceive("ReceiveTimeout", sender, "active")
      // turn Timer off
      context.setReceiveTimeout(Duration.Undefined)
      context.parent ! Timeout
      context stop self

    case Cancel if sender == context.parent =>
      debugReceive("Cancel", sender, "active")
      // turn Timer off
      context.setReceiveTimeout(Duration.Undefined)
      context stop self

  }

  /** Handle unexpected messages */
  def handleUnexpected: Receive = {

    case msg@_ =>
      debugUnexpected(self, sender, "receive", msg)

  }

}

/** Companion object of the BidderTimer agent.
  *
  * Contain all the messages that a BidderTimer agent could receive.
  */
object BidderTimer {

  /** Message Timeout
    *
    * Tell to the timer creator that the countdown is over.
    */
  case class BidderTimeout(id: String)

}

/** BidderTimer agent which makes a countdown.
  *
  * @param duration duration of the timer in milliseconds
  * @param id       identifier of the CFP for which the agent makes a countdown
  */
class BidderTimer(
  duration: Int,
  id: String
) extends Actor with utils.debugs.Debug {

  import Timer._
  import BidderTimer._
  
  this.setDebug(false)

  def receive: Receive = this.active orElse this.handleUnexpected

  /** Active state of the timer. */
  def active: Receive = {

    case Start if sender == context.parent =>
      debugReceive("Start", sender, "active (bidder timer)")
      context.setReceiveTimeout(Duration(this.duration, MILLISECONDS))

    case ReceiveTimeout =>
      debugReceive("ReceiveTimeout", sender, "active (bidder timer)")
      // turn Timer off
      context.setReceiveTimeout(Duration.Undefined)
      debugSend("BidderTimeout", "active (bidder timer)")
      context.parent ! BidderTimeout(this.id)
      context stop self

    case Cancel if sender == context.parent =>
      debugReceive("Cancel", sender, "active (bidder timer)")
      // turn Timer off
      context.setReceiveTimeout(Duration.Undefined)
      context stop self

  }

  /** Handle unexpected messages */
  def handleUnexpected: Receive = {

    case msg@_ =>
      debugUnexpected(self, sender, "receive (bidder timer)", msg)

  }

}
