package utils.jobs.netflix

import utils.jobs._

/** Reduce job */
class RecordByScoreFilmReduceJob extends ReduceJob with StringMapKey
                                                            with IntMapValue
                                                            with StringKey
                                                            with AddInt
                                                            with Pause
