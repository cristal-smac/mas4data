package mapreduce.adaptive

import akka.actor.{ Actor, ActorRef, Props, Stash }
import scala.collection.mutable

import mapreduce.time._
import utils.config.ConfigurationBuilder
import utils.experiments.{ Archive, BrokerData }
import utils.tasks._

/** Companion object of the Broker class.
  *
  * Contain all the messages that a broker could receive.
  */
object Broker {

  // ----- REDUCER ----- //

  /** Message Go
    *
    * Inform the broker that it can start to work.
    */
  object Go

  // ----- MANAGER ----- //

  /** Message ManagerRef
    *
    * Inform the broker of the manager reference.
    *
    * @param ref reference of the manager
    */
  case class ManagerRef(ref: ActorRef)

  /** Message Inform.
    *
    * Inform the broker of the manager contribution value.
    *
    * @param contributionValue current contribution of the manager
    */
  case class Inform(contributionValue: Long)

  /** Message Approved
    *
    * Tell to the broker that the manager accepts to delegate a task.
    */
  object Approve

  /** Message Cancel
    *
    * Tell to the broker that the manager refuses to delegate a task.
    */
  object Cancel

  /** Message Submit
    *
    * Tell to the broker to launch a negotiation for the given task with the
    * given manager contribution.
    *
    * @param task         task to negotiate
    * @param contribution current contribution of the manager
    */
  case class Submit(task: Task, contribution: Long)

  // ----- WHEN BIDDER ----- //

  /** Message CFP
    *
    * Inform the broker about a call for proposal from an other reducer.
    *
    * @param initiator initiator of the call for proposal
    * @param t         negotiated task
    * @param ic        initiator contribution
    * @param id        identifier of the auction
    */
  case class CFP(initiator: ActorRef, t: Task, ic: Long, id: String)

  /** Message Accept
    *
    * Tell to the broker that the initiator of the negotiation accept its offer.
    *
    * @param selected selected reducer
    * @param task     task to transfer to the selected reducer
    * @param id       identifier of the auction
    */
  case class Accept(selected: ActorRef, task: Task, id: String)

  /** Message Reject
    *
    * Tell to the broker that the initiator of the negotiation reject its offer.
    *
    * @param selected selected reducer
    * @param id       identifier of the considered CFP
    */
  case class Reject(selected: ActorRef, id: String)

  // ----- WHEN INITIATOR ----- //

  /** Message Propose
    *
    * Propose to the broker a contribution from a reducer that accept to perform
    * a negotiated task.
    *
    * @param participantContribution contribution of the reducer which accept to
    *                                perform the task
    * @param id                      identifier of the CFP
    */
  case class Propose(ref: ActorRef, participantContribution: Long, id: String)

  /** Message Decline
    *
    * Tell to the broker that another reducer decline the call for proposal.
    *
    * @param ref     message emitter reference
    * @param id      identifier of the auction
    * @param contrib contribution of the bidder
    */
  case class Decline(ref: ActorRef, id: String, contrib: Long)

  /** Message Confirm
    *
    * Tell to the broker that an other reducer confirms that it will perform the
    * negotiated task.
    *
    * @param selected selected reducer
    * @param id       identifier of the auction
    */
  case class Confirm(selected: ActorRef, id: String)

}

/** Broker agent.
  *
  * A broker conducts negotiation with other agents of system.
  *
  * @param acquaintancesSize size of the acquaintances set
  * @param rfhMap            map which associates a reducer to its RFH
  */
class Broker(
  acquaintancesSize: Int,
  rfhMap: Map[ActorRef, ActorRef]
) extends Actor with Stash with utils.debugs.Debug {

  import Broker._

  private val config = ConfigurationBuilder.config

  this.setDebug(this.config.debugs("debug-broker"))

  // Manager of the broker
  private var manager: ActorRef = _

  // Maximum simultaneous auctions as bidder
  private val maxAuctions: Int = {
    val config = this.config.bidderMaxAuction

    if (config.isDefined) config.get else this.acquaintancesSize
  }

  // The number of sent CFP
  private var initCFP = 0

  // The number of canceled CFP (because the worker actually performed the task)
  private var canceledCFP = 0

  // The number of unsuccessful CFP because all the bidders declined
  private var declinedByAllCFP = 0

  // The number of unsuccessful CFP because no proposal before timeout
  private var deniedCFP = 0

  // The number of initiated CFP which are successful
  private var successfulCFP = 0

  // The identifier of the current CFP (ag+initCFP)
  private var idCFP = new String()

  // The set of identifiers of CFPs which have reached the deadline
  private var deprecatedCFP = Set[String]()

  // The current contribution
  private var currentContribution: Long = 0.toLong

  // The total number of received Propose
  private var receivedPropose = 0

  // The cost of the task to delegate
  private var currentTaskCostToDelegate: Long = 0.toLong

  // List of negotiated task costs
  private var negotiatedTaskCosts: List[Long] = Nil

  // Stores the id of the lead negotiations with their associated correspondent
  private val idToRecipient = mutable.Map.empty[String, ActorRef]

  // Archive to record CFP data (CFP ID, nb Propose, nb Decline)
  private val cfpArchive: Archive[(String, Int, Int), (Int, Int, Int)] =
    new Archive[(String, Int, Int), (Int, Int, Int)](self.path.name + "_CFP")

  // Archive to record the fairness benefit for each successful auction
  private val fairnessBenefitArchive: Archive[(Long, Long, Long, Double), Double] =
    new Archive[(Long, Long, Long, Double), Double](
      self.path.name + "_fairness_benefit"
    )

  // Archive to record the cost of delegated tasks
  private val delegationArchive: Archive[Long, Double] =
    new Archive[Long, Double](self.path.name + "_delegation_cost")

  // Get the task cost for another agent
  private def getTaskCostFor(task: Task, agent: ActorRef): Long = {
    task.cost(this.rfhMap(agent))
  }

  private def getTaskCostForSelf(task: Task): Long = {
    this.getTaskCostFor(task, context.parent)
  }

  // Determine if the broker can make a proposal or not
  private def canMakeProposal(
    ownContribution: Long,
    initiatorContribution: Long,
    task: Task
  ): Boolean = {
    val taskCost = this.getTaskCostForSelf(task)
    val x = initiatorContribution - (ownContribution + taskCost)
    val fairnessBenefit = x.toDouble / initiatorContribution

    fairnessBenefit > this.config.threshold
  }

  // Get the fairness benefit from contribution of the initiator, the
  // contribution of the selected bidder and the task cost
  private def getFairnessBenefit(
    ownContribution: Long,
    selectedContribution: Long,
    taskCost: Long
  ): Double = {
    val num = ownContribution - (selectedContribution + taskCost)

    num.toDouble / ownContribution
  }

  // Inform the manager of a given contribution
  private def informManager(
    state: String,
    sender: ActorRef,
    contribution: Long
  ): Unit = {
    debugSend("InformContribution", state)
    this.manager ! Manager.InformContribution(sender, contribution)
  }

  // Pass in the contractor state
  private def goToContractorState(
    selected: ActorRef,
    task: Task,
    state: String,
    storedCfp: Map[ActorRef, CFP]
  ): Unit = {
    val timer = context.actorOf(
      Props(classOf[Timer], this.config.timeouts("contractor-timeout"))
    )

    context become (
      this.contractor(selected, task, timer, storedCfp)
      orElse this.dropProposeDecline("contractor")
      orElse this.processKillMessage("contractor")
      orElse this.handleUnexpected("contractor")
    )
    debugSend("Accept for CFP " + idCFP, state)
    context.parent ! Broker.Accept(selected, task, idCFP)
    timer ! Timer.Start
  }

  // Way the broker concludes the negotiation
  private def endNegotiation(
    task: Task,
    actualContribution: Long,
    proposals: Map[ActorRef, Long],
    nbReplies: Int,
    timer: ActorRef,
    killTimer: Boolean,
    storedCfp: Map[ActorRef, CFP]
  ): Unit = {
    val nbProposals = proposals.size

    // The current CFP is now deprecated
    deprecatedCFP += this.idCFP
    // Update the archive
    this.cfpArchive addRecord (this.idCFP, nbProposals, nbReplies - nbProposals)

    if (killTimer) {
      debug(
        "has received all the reply before the deadline for CFP " + this.idCFP
      )
      timer ! Timer.Cancel
    }

    if (proposals.isEmpty) {
      debug("has received no proposals for CFP " + this.idCFP)

      if (!killTimer) {
        this.deniedCFP += 1
      } else {
        this.declinedByAllCFP += 1
      }

      if (killTimer && storedCfp.nonEmpty) {
        debugSend("CFPDeclinedByAll for CFP " + this.idCFP, "initiator")
        this.manager ! Manager.CFPDeclinedByAll(this.idCFP)
      }

      this.leaveInitiatorState(
        "initiator",
        () => {
          if (!killTimer) {
            // Negotiation ends because of a timeout
            debugSend("BrokerDeny for CFP " + this.idCFP, "initiator")
            this.manager ! Manager.BrokerDeny
          } else {
            // All acquaintances has Declined the CFP
            debugSend("CFPDeclinedByAll for CFP " + this.idCFP, "initiator")
            this.manager ! Manager.CFPDeclinedByAll(this.idCFP)
          }
        },
        storedCfp
      )
    } else {
      debug(
        self.path.name
        + " has received some proposals: "
        + proposals
        + " for CFP "
        + this.idCFP
      )

      // The selected bidder is the one with the lowest workload once it
      // receives the task
      val (selected, contrib) = (proposals.toList sortWith {
        case ((bidder1, c1), (bidder2, c2)) =>
          val bidder1WorkloadWithTask = c1 + this.getTaskCostFor(task, bidder1)
          val bidder2WorkloadWithTask = c2 + this.getTaskCostFor(task, bidder2)

          bidder1WorkloadWithTask < bidder2WorkloadWithTask
      }).head
      val taskCost = this.getTaskCostFor(task, selected)
      val fairnessBenefit = this.getFairnessBenefit(
        actualContribution,
        contrib,
        taskCost
      )

      debug(
        "******** "
        + self.path.name
        + "selected "
        + selected.path.name
        + " with contribution "
        + proposals(selected)
        + " for CFP "
        + this.idCFP
      )
      context become (
        this.awarder(task, selected, storedCfp)
        orElse this.handleDeprecatedProposeFromSelected(selected, "awarder")
        orElse this.handleDeprecatedTimeout("awarder")
        orElse this.dropProposeDecline("awarder")
        orElse this.processKillMessage("awarder")
        orElse this.handleUnexpected("awarder")
      )
      this.fairnessBenefitArchive.addRecord(
        (actualContribution, contrib, taskCost, fairnessBenefit)
      )
      debugSend("BrokerReady for CFP " + this.idCFP, "initiator")
      this.manager ! Manager.BrokerReady(task)
      // Reject all the not selected bidder
      (proposals.keys filterNot { _ == selected }) foreach { notSelected =>
        debugSend("Reject", "initiator")
        context.parent ! Broker.Reject(notSelected, idCFP)
      }
    }
  }

  // Handle CFP from other initiator when the broker is itself initiator
  private def handleCfpAsInitiator(
    cfp: CFP,
    storedCfp: Map[ActorRef, CFP],
    state: String
  ): Map[ActorRef, CFP] =
    if (!this.canMakeProposal(this.currentContribution, cfp.ic, cfp.t)) {
      debugSend("Decline CFP " + cfp.id, state)
      context.parent ! Broker.Decline(
        cfp.initiator,
        cfp.id,
        this.currentContribution
      )
      storedCfp
    } else {
      debug("store CFP " + cfp.id)
      storedCfp + (cfp.initiator -> cfp)
    }

  // Leaving initiator state mechanism
  private def leaveInitiatorState(
    state: String,
    actionToDoIfNoStoredCfp: () => Unit,
    storedCfp: Map[ActorRef, CFP]
  ): Unit =
    // If there is no stored CFP, the initiator switch to broker state
    if (storedCfp.isEmpty) {
      actionToDoIfNoStoredCfp()
      context become (
        this.broker
        orElse this.dropProposeDecline("broker")
        orElse this.handleDeprecatedTimeout("broker")
        orElse this.processKillMessage("broker")
        orElse this.handleUnexpected("broker")
      )
    }
    // Else, there are stored CFP, the broker switch to bidder state
    else {
      context become (
        this.bidder(
          storedCfp,
          None,
          0.toLong,
          Map[String, CFP](),
          Map[String, (ActorRef, Propose)]()
        )
        orElse this.handleDeprecatedSubmit("bidder")
        orElse this.dropProposeDecline("bidder")
        orElse this.processKillMessage("bidder")
        orElse this.handleUnexpected("bidder")
      )
      debugSend("QueryContribution", state)
      this.manager ! Manager.QueryContribution
    }

  def receive: Receive =
    this.waitManager orElse this.handleUnexpected("waitManager")

  /** State in which the broker waits its manager reference. */
  def waitManager: Receive = {

    case ManagerRef(ref) =>
      this.manager = ref
      context become (
        this.waitGo
        orElse this.handleNotReady("waitGo")
      )
      debugSend("Ready", "waitManager")
      context.parent ! AdaptiveReducer.Ready

  }

  /** State in which the broker waits the reducer green light. */
  def waitGo: Receive = {

    case Go =>
      unstashAll()
      context become (
         this.broker
         orElse this.dropProposeDecline("broker")
         orElse this.processKillMessage("broker")
         orElse this.handleUnexpected("broker")
       )

  }

  /** Initial state of a broker. */
  def broker: Receive = {

    case msg@CFP(initiator, _, ic, id) =>
      debugReceive("CFP from " + initiator.path.name, sender, "broker")
      this.idToRecipient += (id -> initiator)
      context become (
        this.bidder(
          Map(initiator -> msg),
          None,
          0.toLong,
          Map[String, CFP](),
          Map[String, (ActorRef, Propose)]()
        )
        orElse this.handleDeprecatedSubmit("bidder")
        orElse this.dropProposeDecline("bidder")
        orElse this.processKillMessage("bidder")
        orElse this.handleUnexpected("bidder")
      )
      debugSend("QueryContribution", "broker")
      this.manager ! Manager.QueryContribution
      this.informManager("broker", initiator, ic)

    case Submit(task, contribution) =>
      debugReceive("Submit for key " + task.key, sender, "broker")
      this.currentContribution = contribution
      this.currentTaskCostToDelegate = this.getTaskCostForSelf(task)
      this.idCFP = self.path.name + "_" + this.initCFP.toString

      val timer = context.actorOf(
        Props(classOf[Timer], this.config.timeouts("initiator-timeout"))
      )

      context become (
        this.initiator(
          task,
          contribution,
          List[ActorRef](),
          Map[ActorRef, Long](),
          timer,
          Map[ActorRef, CFP]()
        )
        orElse this.dropProposeDecline("initiator")
        orElse this.processKillMessage("initiator")
        orElse this.handleUnexpected("initiator")
      )
      debugSend(
        "CFP with id "
        + this.idCFP
        + " and tc="
        + currentTaskCostToDelegate
        + " ic="
        + contribution,
        "broker"
      )
      context.parent ! Broker.CFP(
        self,
        task,
        contribution,
        this.idCFP
      )
      this.initCFP += 1
      timer ! Timer.Start

  }

  // ---------- NEGOTIATION AS PARTICIPANT ---------- //

  /** Bidder state of a broker.
    *
    * In this state the broker wait a response from the initiators of the
    * current negotiations and handles new ones
    *
    * @param storedCfp        stored cfp potentially answerable
    * @param realContribution current contribution of the reducer
    * @param virtualOverhead  overhead represented by the ongoing negotiations
    * @param currentCfp       ongoing negotiations
    * @param timers           timers used in current auctions
    */
  def bidder(
    storedCfp: Map[ActorRef, CFP],
    realContribution: Option[Long],
    virtualOverhead: Long,
    currentCfp: Map[String, CFP],
    timers: Map[String, (ActorRef, Propose)]
  ): Receive = {

    // ----------- FROM MANAGER ---------- //

    // This message causes the stored CFP revision by the bidder
    case Inform(contribution) =>
      debugReceive(
        "Inform with contribution " + contribution,
        sender,
        "bidder\n\tcurrent: " + currentCfp + "\n\tstored: " + storedCfp
      )

      // The bidder answers to the most loaded initiators first
      val orderedStoredCfp = storedCfp.toList sortWith {
        case ((_, cfp1), (_, cfp2)) => cfp1.ic > cfp2.ic
      }
      // Browse each CFP and propose/decline when it is possible
      val (newStored, newVo, newCurrent, newTimers) =
        orderedStoredCfp.foldLeft(
          (Map[ActorRef, CFP](), virtualOverhead, currentCfp, timers)
        ) {
          case (
            // Accumulators
            (stored, vo, current, accTimers),
            // Current CFP
            (_, cfp@CFP(initiator, t, ic, id))
          ) =>
            val canMakeProposal = this.canMakeProposal(contribution + vo, ic, t)

            // The bidder is able to make a proposal for the current CFP
            if ((current.size < this.maxAuctions) && canMakeProposal) {
              val sentContribution = contribution + vo
              val timer = context actorOf Props(
                classOf[BidderTimer],
                this.config.timeouts("bidder-timeout"),
                id
              )
              val prop = Propose(initiator, sentContribution, id)

              debugSend(
                "Propose for CFP " +
                id +
                " with contribution " +
                sentContribution,
                "bidder"
              )
              context.parent ! prop
              timer ! Timer.Start
              (
                stored,
                vo + this.getTaskCostForSelf(t),
                // The CFP is added to the current CFP
                current + (id -> cfp),
                accTimers + (id -> (timer, prop))
              )
            }
            // The bidder could handle the CFP depending on the ongoing auctions
            else if (canMakeProposal) {
              debug("store CFP " + id)
              (stored + (initiator -> cfp), vo, current, accTimers)
            }
            // The bidder can not handle the CFP
            else {
              debugSend("Decline CFP " + id, "bidder")
              context.parent ! Decline(initiator, id, contribution)
              (stored, vo, current, accTimers)
            }
        }

      // If there is no more current or stored CFP, the bidder switches in
      // broker state
      if (newCurrent.isEmpty && newStored.isEmpty) {
        debugSend("BrokerNotBusy", "bidder")
        this.manager ! Manager.BrokerNotBusy
        context become (
          this.broker
          orElse this.handleDeprecatedReject("broker")
          orElse this.handleDeprecatedInform("broker")
          orElse this.dropProposeDecline("broker")
          orElse this.handleDeprecatedTimeout("broker")
          orElse this.processKillMessage("broker")
          orElse this.handleUnexpected("broker")
        )
      }
      // Else, it remains current or stored CFP, the bidder stays in bidder
      // state
      else {
        context become (
          this.bidder(
            newStored,
            Some(contribution),
            newVo,
            newCurrent,
            newTimers
          )
          orElse this.handleDeprecatedSubmit("bidder")
          orElse this.handleDeprecatedAccept("bidder")
          orElse this.dropProposeDecline("bidder")
          orElse this.processKillMessage("bidder")
          orElse this.handleUnexpected("bidder")
        )
      }


    // ---------- FROM INITIATORS ---------- //

    // The bidder is aware of the actual contribution
    case cfp@CFP(initiator, t, ic, id) if realContribution.isDefined =>
      debugReceive(
        "CFP from " + initiator.path.name,
        sender,
        "bidder\n\tcurrent: " + currentCfp + "\n\tstored: " + storedCfp
      )
      this.idToRecipient += (id -> initiator)
      this.informManager("bidder", initiator, ic)

      val contribution = realContribution.get

      // The bidder can handle the auction
      if (
        (currentCfp.size < this.maxAuctions) &&
        this.canMakeProposal(contribution + virtualOverhead, ic, t)
      ) {
        val sentContribution = contribution + virtualOverhead
        val newVo = virtualOverhead + this.getTaskCostForSelf(t)
        val newCurrent = currentCfp + (id -> cfp)
        val timer = context actorOf Props(
          classOf[BidderTimer],
          this.config.timeouts("bidder-timeout"),
          id
        )
        val prop = Propose(initiator, sentContribution, id)

        debugSend(
          "Propose for CFP " + id + " with contribution " + sentContribution,
          "bidder"
        )
        context.parent ! prop
        timer ! Timer.Start
        context become (
          this.bidder(
            storedCfp - initiator,
            realContribution,
            newVo,
            newCurrent,
            timers + (id -> (timer, prop))
          )
          orElse this.handleDeprecatedSubmit("bidder")
          orElse this.handleDeprecatedAccept("bidder")
          orElse this.dropProposeDecline("bidder")
          orElse this.processKillMessage("bidder")
          orElse this.handleUnexpected("bidder")
        )
      }
      // The bidder can handle the auction depending on the ongoing auctions
      else if (this.canMakeProposal(contribution, ic, t)) {
        val newStored = storedCfp + (initiator -> cfp)

        debug("store CFP " + id)
        context become (
          this.bidder(
            newStored,
            realContribution,
            virtualOverhead,
            currentCfp,
            timers
          )
          orElse this.handleDeprecatedSubmit("bidder")
          orElse this.handleDeprecatedAccept("bidder")
          orElse this.dropProposeDecline("bidder")
          orElse this.processKillMessage("bidder")
          orElse this.handleUnexpected("bidder")
        )
      }
      // The bidder can not handle the auction
      else {
        debugSend("Decline CFP " + id, "bidder")
        context.parent ! Decline(initiator, id, contribution)
        context become (
          this.bidder(
            storedCfp - initiator,
            realContribution,
            virtualOverhead,
            currentCfp,
            timers
          )
          orElse this.handleDeprecatedSubmit("bidder")
          orElse this.handleDeprecatedAccept("bidder")
          orElse this.dropProposeDecline("bidder")
          orElse this.processKillMessage("bidder")
          orElse this.handleUnexpected("bidder")
        )
      }

    // The bidder waits the actual contribution from the manager
    case cfp@CFP(initiator, _, ic, id) =>
      debugReceive(
        "CFP from " + initiator.path.name,
        sender,
        "bidder\n\tcurrent: " + currentCfp + "\n\tstored: " + storedCfp
      )
      debug("store CFP " + id)
      this.idToRecipient += (id -> initiator)
      this.informManager("broker", initiator, ic)

      val newStored = storedCfp + (initiator -> cfp)

      context become (
        this.bidder(
          newStored,
          realContribution,
          virtualOverhead,
          currentCfp,
          timers
        )
        orElse this.handleDeprecatedSubmit("bidder")
        orElse this.handleDeprecatedAccept("bidder")
        orElse this.dropProposeDecline("bidder")
        orElse this.processKillMessage("bidder")
        orElse this.handleUnexpected("bidder")
      )

    case Accept(_, task, id) if currentCfp contains id =>
      debugReceive(
        "Accept for CFP " + id,
        sender,
        "bidder\n\tcurrent: " + currentCfp + "\n\tstored: " + storedCfp
      )

      val initiator = currentCfp(id).initiator
      val newCurrentCfp = currentCfp - id
      val timer = timers(id)._1

      debugSend("Confirm for CFP " + id, "bidder")
      context.parent ! Broker.Confirm(initiator, id)
      timer ! Timer.Cancel

      // No more CFP to handle
      if (storedCfp.isEmpty && newCurrentCfp.isEmpty) {
        debugSend("RequestAndNotBusy", "bidder")
        this.manager ! Manager.RequestAndNotBusy(task)
        context become (
          this.broker
          orElse this.handleDeprecatedReject("broker")
          orElse this.handleDeprecatedInform("broker")
          orElse this.dropProposeDecline("broker")
          orElse this.processKillMessage("broker")
          orElse this.handleUnexpected("broker")
        )
      }
      // Remaining CFP
      else {
        val newVo = virtualOverhead - this.getTaskCostForSelf(task)
        val newTimers = timers - id

        debugSend("Request for CFP " + id, "bidder")
        this.manager ! Manager.Request(task)
        debugSend("QueryContribution", "bidder")
        this.manager ! Manager.QueryContribution
        context become (
          // No current contribution
          // Waiting the new contribution from the manager
          this.bidder(storedCfp, None, newVo, newCurrentCfp, newTimers)
          orElse this.handleDeprecatedSubmit("bidder")
          orElse this.handleDeprecatedAccept("bidder")
          orElse this.dropProposeDecline("bidder")
          orElse this.processKillMessage("bidder")
          orElse this.handleUnexpected("bidder")
        )
      }

    case Reject(_, id) if currentCfp contains id =>
      debugReceive(
        "Reject for CFP " + id,
        sender,
        "bidder\n\tcurrent: " + currentCfp + "\n\tstored: " + storedCfp
      )

      val newCurrentCfp = currentCfp - id
      val timer = timers(id)._1

      timer ! Timer.Cancel

      // No more CFP to handle
      if (storedCfp.isEmpty && newCurrentCfp.isEmpty) {
        debugSend("BrokerNotBusy", "bidder")
        this.manager ! Manager.BrokerNotBusy
        context become (
          this.broker
          orElse this.handleDeprecatedReject("broker")
          orElse this.handleDeprecatedInform("broker")
          orElse this.dropProposeDecline("broker")
          orElse this.handleDeprecatedTimeout("broker")
          orElse this.processKillMessage("broker")
          orElse this.handleUnexpected("broker")
        )
      }
      // Remaining CFP
      else {
        val newVo = virtualOverhead - this.getTaskCostForSelf(currentCfp(id).t)
        val newTimers = timers - id

        debugSend("QueryContribution", "bidder")
        this.manager ! Manager.QueryContribution
        context become (
          // No currentContribution
          // Waiting the new contribution from the manager
          this.bidder(storedCfp, None, newVo, newCurrentCfp, newTimers)
          orElse this.handleDeprecatedSubmit("bidder")
          orElse this.handleDeprecatedAccept("bidder")
          orElse this.dropProposeDecline("bidder")
          orElse this.processKillMessage("bidder")
          orElse this.handleUnexpected("bidder")
        )
      }

    // ---------- BIDDER TIMEOUT ---------- //

    case BidderTimer.BidderTimeout(id) if timers contains id =>
      debugReceive("BidderTimeout for CFP " + id, sender, "bidder")

      val (_, prop) = timers(id)
      val newTimer = context actorOf Props(
        classOf[BidderTimer],
        this.config.timeouts("bidder-timeout"),
        id
      )

      debugSend("Propose for CFP " + id, "bidder")
      context.parent ! prop
      newTimer ! Timer.Start
      context become (
        this.bidder(
          storedCfp,
          realContribution,
          virtualOverhead,
          currentCfp,
          timers + (id -> (newTimer, prop))
        )
        orElse this.handleDeprecatedSubmit("bidder")
        orElse this.handleDeprecatedAccept("bidder")
        orElse this.dropProposeDecline("bidder")
        orElse this.processKillMessage("bidder")
        orElse this.handleUnexpected("bidder")
      )

  }

  // ---------- NEGOTIATION AS INITIATOR ---------- //

  /** Initiator state of the broker.
    *
    * In this state the broker initialize a negotiation for a task.
    *
    * @param task               task to negotiate
    * @param actualContribution actual contribution of the reducer
    * @param replies            acquaintances which have replied to the current
    *                           CFP
    * @param proposals          save of the proposals by the other adaptive
    *                           reducers
    * @param timer              current timer
    * @param storedCfp          stored CFP to be handled later
    */
  def initiator(
    task: Task,
    actualContribution: Long,
    replies: List[ActorRef],
    proposals: Map[ActorRef, Long],
    timer: ActorRef,
    storedCfp: Map[ActorRef, CFP]
  ): Receive = {

    // New propose from a bidder
    case Propose(
      ref,
      contributionValue,
      id
    ) if (id == this.idCFP) && !(replies contains ref) =>
      debugReceive(
        "Propose relevant from "
        + ref.path.name + " for CFP "
        + id
        + " with contribution of "
        + contributionValue,
        sender,
        "initiator"
      )
      this.receivedPropose += 1

      val newReplies = ref :: replies
      val newProposals = proposals + (ref -> contributionValue)

      if (newReplies.lengthCompare(this.acquaintancesSize) == 0) {
        debug("has received all reply for CFP" + id)
        this.endNegotiation(
          task,
          actualContribution,
          newProposals,
          newReplies.length,
          timer,
          killTimer = true,
          storedCfp
        )
      } else {
        context become (
          this.initiator(
            task,
            actualContribution,
            newReplies,
            newProposals,
            timer,
            storedCfp
          )
          orElse this.dropProposeDecline("initiator")
          orElse this.processKillMessage("initiator")
          orElse this.handleUnexpected("initiator")
        )
      }

    // Propose already received
    case Propose(ref, _, id) if (id == this.idCFP) && (replies contains ref) =>
      debugReceive(
        "Propose already received from "
        + ref.path.name + " for CFP "
        + id + ", nothing to do",
        sender,
        "initiator"
      )

    case Decline(ref, id, contrib) if (id == this.idCFP) && !(replies contains ref) =>
      debugReceive(
        "Decline relevant from " + ref.path.name + " for CFP " + id,
        sender,
        "initiator"
      )
      this.informManager("initiator", ref, contrib)

      val newReplies = sender :: replies

      if (newReplies.lengthCompare(this.acquaintancesSize) == 0) {
        debug("has received all reply for CFP" + id)
        this.endNegotiation(
          task,
          actualContribution,
          proposals,
          newReplies.length,
          timer,
          killTimer = true,
          storedCfp
        )
      } else {
        context become (
          this.initiator(
            task,
            actualContribution,
            newReplies,
            proposals,
            timer,
            storedCfp
          )
          orElse this.dropProposeDecline("initiator")
          orElse this.processKillMessage("initiator")
          orElse this.handleUnexpected("initiator")
        )
      }

    case Decline(ref, id, contrib) if (id == this.idCFP) && (replies contains ref) =>
      debugReceive(
        "Decline already received from " +
        ref.path.name +
        " for CFP " +
        id +
        ", nothing to do",
        sender,
        "initiator"
      )
      this.informManager("initiator", ref, contrib)

    case Timer.Timeout if sender == timer =>
      debugReceive("Timeout for CFP " + idCFP, sender, "initiator")
      this.endNegotiation(
        task,
        actualContribution,
        proposals,
        replies.length,
        timer,
        killTimer = false,
        storedCfp
      )

    case msg@CFP(initiator, _, ic, _) =>
      debugReceive("CFP from " + initiator.path.name, sender, "initiator")
      this.informManager("initiator", initiator, ic)
      context become (
        this.initiator(
          task,
          actualContribution,
          replies,
          proposals,
          timer,
          this.handleCfpAsInitiator(msg, storedCfp, "initiator")
        )
        orElse this.dropProposeDecline("initiator")
        orElse this.processKillMessage("initiator")
        orElse this.handleUnexpected("initiator")
      )

  }

  /** Awarder state of the broker.
    *
    * In this state the broker wait the confirmation of the manager to delegate
    * the negotiated task.
    *
    * @param task      negotiated task
    * @param selected  selected reducer to perform the negotiated task
    * @param storedCfp stored CFP to be handled later
    */
  def awarder(
    task: Task,
    selected: ActorRef,
    storedCfp: Map[ActorRef, CFP]
  ): Receive = {

    case Approve =>
      debugReceive("Approved for CFP " + idCFP, sender, "awarder")
      this.goToContractorState(selected, task, "awarder", storedCfp)

    case Cancel =>
      debugReceive("Cancel for CFP " + idCFP, sender, "awarder")
      this.canceledCFP += 1
      debugSend("Reject for CFP " + idCFP, "awarder")
      context.parent ! Broker.Reject(selected, idCFP)
      this.leaveInitiatorState(
        "awarder",
        () => {},
        storedCfp
      )

    case msg@CFP(initiator, _, ic, _) =>
      debugReceive("CFP from " + initiator.path.name, sender, "awarder")
      this.informManager("awarder", initiator, ic)
      context become (
        this.awarder(
          task,
          selected,
          this.handleCfpAsInitiator(msg, storedCfp, "awarder")
        )
        orElse this.handleDeprecatedProposeFromSelected(selected, "awarder")
        orElse this.handleDeprecatedTimeout("awarder")
        orElse this.dropProposeDecline("awarder")
        orElse this.processKillMessage("awarder")
        orElse this.handleUnexpected("awarder")
      )

  }

  /** Contractor state of a broker.
    *
    * In this state the broker wait the confirmation of the selected reducer.
    *
    * @param selected  selected bidder for the negotiation
    * @param task      negotiated task
    * @param timer     current timer
    * @param storedCfp stored CFP to be handled later
    */
  def contractor(
    selected: ActorRef,
    task: Task,
    timer: ActorRef,
    storedCfp: Map[ActorRef, CFP]
  ): Receive = {

    case Confirm(_, id) =>
      debugReceive("Confirm for CFP" + id, sender, "contractor")

      val delegatedTaskCost = this.getTaskCostForSelf(task)

      this.currentContribution -= delegatedTaskCost
      this.successfulCFP += 1
      timer ! Timer.Cancel
      this.negotiatedTaskCosts = delegatedTaskCost :: this.negotiatedTaskCosts
      this.delegationArchive.addRecord(task.nbValues)
      this.leaveInitiatorState(
        "contractor",
        () => {
          debugSend("BrokerFinish for CFP " + id, "contractor")
          this.manager ! Manager.BrokerFinish
        },
        storedCfp
      )

    case Timer.Timeout if sender == timer =>
      debugReceive("Timer.Timeout", sender, "contractor")
      goToContractorState(selected, task, "contractor", storedCfp)

    case msg@CFP(initiator, _, ic, _) =>
      debugReceive("CFP from " + initiator.path.name, sender, "contractor")
      this.informManager("contractor", initiator, ic)
      context become (
        this.contractor(
          selected,
          task,
          timer,
          this.handleCfpAsInitiator(msg, storedCfp, "contractor")
        )
        orElse this.dropProposeDecline("contractor")
        orElse this.processKillMessage("contractor")
        orElse this.handleUnexpected("contractor")
      )

  }

  /** Permanent state which allows to drop the reply to the deprecated
    * Propose/Decline messages.
    *
    * @param state state in which the Propose/Decline message to drop is
    *              received
    */
  def dropProposeDecline(state: String): Receive = {

    case Propose(bidder, _, id) =>
      debugDeprecated("Propose for CFP " + id, sender, state)
      debugSend("Reject for CFP " + id, state)
      context.parent ! Broker.Reject(bidder, id)

    case Decline(ref, id, contrib) =>
      debugDeprecated("Decline for CFP " + id, sender, state)
      this.informManager("dropProposeDecline", ref, contrib)

  }

  /** Permanent state which allows the broker to process the Kill message
    * from its reducer.
    *
    * @param state state in which the Kill message is received
    */
  def processKillMessage(state: String): Receive = {

    case AdaptiveReducer.Kill =>
      debugReceive("Kill", sender, state)

      val brokerData = new BrokerData(
        this.initCFP,
        this.canceledCFP,
        this.deniedCFP,
        this.declinedByAllCFP,
        this.successfulCFP,
        this.receivedPropose,
        this.negotiatedTaskCosts.sum.toDouble / this.negotiatedTaskCosts.length
      )

      debugSend("BrokerKillOk", state)
      sender ! AdaptiveReducer.BrokerKillOk(brokerData)
      this.cfpArchive.writeCSV(
        "CFP ID;Number of Proposal, Number of Decline",
        { case (id, prop, declined) => id + ";" + prop + ";" + declined }
      )
      this.fairnessBenefitArchive.writeCSV(
        "Initiator contribution;Bidder contribution;Task cost;Fairness benefit",
        { case (i, b, t, f) => i + ";" + b + ";" + t + ";" + f }
      )
      this.delegationArchive.writeCSV(
        "Task cost",
        _.toString
      )
      brokerData.write(this.config.resultPath + self.path.name + ".csv")
      context stop self

  }

  /** Handle the deprecated Propose messages from the selected participant of a
    * negotiation.
    *
    * @param selected selected participant of a negotiation
    * @param state    state in which the deprecated Propose message is received
    */
  def handleDeprecatedProposeFromSelected(
    selected: ActorRef,
    state: String
  ): Receive = {

    case Propose(bidder, _, _) if bidder == selected =>
      debugDeprecated(
        "Propose from selected for CFP " + idCFP,
        sender,
        "awarder"
      )

  }

  /** Handle deprecated Timeout messages.
    *
    * @param state state in which the deprecated timeout message is received
    */
  def handleDeprecatedTimeout(state: String): Receive = {

    case Timer.Timeout =>
      debugDeprecated("Timeout", sender, state)

  }

  /** Handle deprecated Confirm messages.
    *
    * @param state state in which the deprecated Confirm message is received
    */
  def handleDeprecatedConfirm(state: String): Receive = {

    case Confirm(selected, id) =>
      debugDeprecated(
        "Confirm for CFP" + id + " from " + selected,
        sender,
        state
      )

  }

  /** Handle deprecated Reject messages.
    *
    * @param state in which the deprecated Reject message is received
    */
  def handleDeprecatedReject(state: String): Receive = {

    case Reject(selected, id) =>
      debugDeprecated(
        "Reject for CFP " + id + " from " + selected,
        sender,
        state
      )

  }

  /** Handle deprecated Accept messages.
    *
    * @param state in which the deprecated Accept message is received
    */
  def handleDeprecatedAccept(state: String): Receive = {

    case Accept(_,_,id) if this.idToRecipient contains id =>
      debugDeprecated("Accept for CFP " + id, sender, state)
      context.parent ! Confirm(this.idToRecipient(id), id)

  }

  /** Handle deprecated Submit messages.
    *
    * @param state state in which the deprecated Submit message is received
    */
  def handleDeprecatedSubmit(state: String): Receive = {

    case Submit(task, _) =>
      debugDeprecated("Submit for key " + task.key, sender, state)

  }

  /** Handle deprecated Inform messages.
    *
    * @param state state in which the deprecated Inform message is received
    */
  def handleDeprecatedInform(state: String): Receive = {

    case Inform(_) => debugDeprecated("Inform", sender, state)

  }

  /** Handle unexpected messages.
    *
    * @param state state in which the unexpected message is received
    */
  def handleUnexpected(state: String): Receive = {

    case msg@_ => debugUnexpected(self, sender, state, msg)

  }

  /** Handle messages while the sub-agents of the reducers are not yet ready.
    *
    * @param state state in which the unexpected messages is received
    */
  def handleNotReady(state: String): Receive = {

    case msg@CFP(initiator, _, ic, _) =>
      debugReceive("CFP", sender, "waitGo")
      this.informManager("waitGo", initiator, ic)
      debug("stash " + msg + " because not ready to handle it")
      stash()

    case msg@_ =>
      debug("stash " + msg + " because not ready to handle it")
      stash()

  }

}
