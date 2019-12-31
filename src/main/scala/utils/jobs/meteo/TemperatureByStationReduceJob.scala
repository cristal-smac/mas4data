package utils.jobs.meteo

import utils.jobs._

class TemperatureByStationReduceJob extends ReduceJob
                                    with StringMapKey
                                    with DoubleIntTupleMapValue
                                    with StringKey
                                    with MeanDouble
