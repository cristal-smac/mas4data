package utils.debugs

import akka.actor.{Actor, ActorRef}

/** Companion object of the Debug trait. */
object Debug { val PROMPT = " >>>>> " }

/** Tool for displaying debug trace, actor's name is added before each message.
  */
trait Debug extends Actor {

  // Flag to determine if the debug mode is active or not
  private var debugFlag = true

  /** Enable or disable debug trace.
    *
    * @param flag when <code>true</code> debug trace is enabled
    */
  def setDebug(flag: Boolean): Unit = {
    this.debugFlag = flag
  }

  /** Display a message in debug trace
    *
    * @param msg the message to display
    */
  def debug(msg: String): Unit =
    if (debugFlag) println(Debug.PROMPT + self.path.name + " " + msg)

  /** Display a received msg with state information.
    *
    * @param msg   the displayed message
    * @param state the actor's state
    */
  def debugReceive(msg: String, sender: ActorRef, state: String): Unit = {
    debug(": " + msg + " from " + sender.path.name + " when " + state)
  }

  /** Display a sent msg with state information.
    *
    * @param msg   the displayed message
    * @param state the actor's state
    */
  def debugSend(msg: String, state: String): Unit =
    debug("! " + msg + " when " + state)

  /** Display a deprecated message with state and sender information.
    *
    * @param msg    the displayed message
    * @param sender the deprecated message sender
    * @param state  the actor's state
    */
  def debugDeprecated(msg: String, sender: ActorRef, state: String): Unit =
    debug(
      ": [deprecated] " + msg + " from " + sender.path.name + " when " + state
    )

  /** Display a unexpected message with state and sender information.
    *
    * @param msg    the displayed message
    * @param sender the unexpected message sender
    * @param state  the actor's state
    */
  def debugUnexpected(
    ref: ActorRef,
    sender: ActorRef,
    state: String,
    msg: Any
  ): Unit =
    println(
      ref.path.name + " ("+state+") does not process " +msg + " from " +
        sender.path.name
    )

}
