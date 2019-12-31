package utils.jobs.sales

import utils.jobs._

/** Reduce job for the count key problem. */
class RevenueReduceJob extends ReduceJob with StringMapKey
                                         with DoubleMapValue
                                         with StringKey
                                         with AddDouble
                                         with Pause
