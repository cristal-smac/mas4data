package utils.jobs.yahoo

import utils.jobs._

/** Reduce job. */
class CountByKeywordReduceJob extends ReduceJob with StringMapKey
                                                with IntMapValue
                                                with StringKey
                                                with AddInt
                                                with Pause
