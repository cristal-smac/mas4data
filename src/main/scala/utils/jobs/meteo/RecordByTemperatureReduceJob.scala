package utils.jobs.meteo

import utils.jobs._

/** Reduce job for the count key problem. */
class RecordByTemperatureReduceJob extends ReduceJob with DoubleMapKey
                                                     with IntMapValue
                                                     with DoubleKeyFromDoubleMapKey
                                                     with AddInt
                                                     with Pause
