package utils.config

import utils.strategies.taskCost.{TaskCostStrategy, TaskCostStrategyHardwareMeasures, TaskCostStrategyNbValues, TaskCostStrategyRemoteChunkMultiplier}

object TaskCostStrategyHandler {

  // Pattern to identify the remote chunk multiplier task cost strategy
  private val multiplierPattern = """\(multiplier, (\d+)\)""".r

  // Pattern to identify the hardware measure task cost strategy
  private val hardwarePattern =
    """\(hardware, rd=(\d+), wd=(\d+), nd=(\d+), hdl=(\d+), nvps=(\d+)\)""".r

  def getTaskCostStrategyFrom(strategy: String): TaskCostStrategy =
    strategy match {
      case this.multiplierPattern(mult)                =>
        TaskCostStrategyRemoteChunkMultiplier(mult.toInt)

      case this.hardwarePattern(rd, wd, nd, hdl, nvps) =>
        TaskCostStrategyHardwareMeasures(
          rd.toInt,
          wd.toInt,
          nd.toInt,
          hdl.toInt,
          nvps.toInt
        )

      case _                                           =>
        TaskCostStrategyNbValues
    }

}
