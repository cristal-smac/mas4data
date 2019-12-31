package utils.jobs.netflix

import utils.jobs._

class ScoreByFilmReduceJob extends ReduceJob
                                    with StringMapKey
                                    with DoubleIntTupleMapValue
                                    with StringKey
                                    with MeanDouble
