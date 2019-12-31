package utils.jobs.netflix

import utils.jobs._

/** Reduce job */
class RecordByDayMonthReduceJob extends ReduceJob with StringMapKey
                                                            with IntMapValue
                                                            with StringKey
                                                            with AddInt
                                                            with Pause
