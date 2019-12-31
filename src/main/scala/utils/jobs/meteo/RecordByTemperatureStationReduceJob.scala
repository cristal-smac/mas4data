package utils.jobs.meteo

import utils.jobs._

/** Reduce job */
class RecordByTemperatureStationReduceJob extends ReduceJob with StringMapKey
                                                            with IntMapValue
                                                            with StringKey
                                                            with AddInt
                                                            with Pause
