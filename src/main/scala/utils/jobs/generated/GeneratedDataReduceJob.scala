package utils.jobs.generated

import utils.jobs._

/** Reduce job for generated data. */
class GeneratedDataReduceJob extends ReduceJob with StringMapKey
                                               with IntIntTupleMapValue
                                               with StringKey
                                               with MeanInt
                                               with Pause
