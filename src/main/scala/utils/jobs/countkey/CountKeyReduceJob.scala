package utils.jobs.countkey

import utils.jobs._

/** Reduce job for the count key problem. */
class CountKeyReduceJob extends ReduceJob with IntMapKey
                                          with IntMapValue
                                          with IntKeyFromIntMapKey
                                          with AddInt
                                          with Pause
