package utils.jobs.netflix

import utils.jobs._

/** Reduce job for the count key problem. */
class RecordByFilmReduceJob extends ReduceJob with DoubleMapKey
                                                     with IntMapValue
                                                     with DoubleKeyFromDoubleMapKey
                                                     with AddInt
                                                     with Pause
