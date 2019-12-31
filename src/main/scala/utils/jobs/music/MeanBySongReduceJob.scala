package utils.jobs.music

import utils.jobs._

class MeanBySongReduceJob extends ReduceJob with IntMapKey
                                            with IntIntTupleMapValue
                                            with IntKeyFromIntMapKey
                                            with MeanInt
