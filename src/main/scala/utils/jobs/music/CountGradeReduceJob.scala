package utils.jobs.music

import utils.jobs._

class CountGradeReduceJob extends ReduceJob with IntMapKey
                                            with IntMapValue
                                            with IntKeyFromIntMapKey
                                            with AddInt
                                            with Pause
