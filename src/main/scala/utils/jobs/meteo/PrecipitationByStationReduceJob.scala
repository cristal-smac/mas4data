package utils.jobs.meteo

import utils.jobs._

/** Reduce job for the count key problem. */
class PrecipitationByStationReduceJob extends ReduceJob with StringMapKey
                                                        with DoubleMapValue
                                                        with StringKey
                                                        with AddDouble
                                                        with Pause
