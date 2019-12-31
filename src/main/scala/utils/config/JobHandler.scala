package utils.config

object JobHandler {

  private val prefix = "utils.jobs."

  // Job class name to job class instance
  private def toClass(jobClassNames: (String, String)): (Class[_], Class[_]) = (
    Class.forName(this.prefix + jobClassNames._1),
    Class.forName(this.prefix + jobClassNames._2)
  )

  /** Create the map and reduce jobs from the job name of the configuration
    * file.
    *
    * @param job job name of the configuration file
    * @return Map and reduce jobs associated with the job name
    */
  def getJobsFrom(job: String): (Class[_], Class[_]) =
    this toClass (job match {
      case "netflixRecordBySomeFilms"        => (
        "netflix.RecordBySomeFilmsMapJob",
        "netflix.RecordByFilmReduceJob"
      )
      case "netflixRecordByScoreFilm"        => (
        "netflix.RecordByScoreFilmMapJob",
        "netflix.RecordByScoreFilmReduceJob"
      )
      case "netflixRecordByDayMonth"        => (
        "netflix.RecordByDayMonthMapJob",
        "netflix.RecordByDayMonthReduceJob"
      )
      case "netflixScoreByFilm"        => (
        "netflix.ScoreByFilmMapJob",
        "netflix.ScoreByFilmReduceJob"
      )
      case "netflixRecordByFilm"        => (
        "netflix.RecordByFilmMapJob",
        "netflix.RecordByFilmReduceJob"
      )
      case "countkey"                        => (
        "countkey.CountKeyMapJob",
        "countkey.CountKeyReduceJob"
      )

      case "meteoPrecipitationByStation"     => (
        "meteo.PrecipitationByStationMapJob",
        "meteo.PrecipitationByStationReduceJob"
      )

      case "meteoTemperatureByStation"       => (
        "meteo.TemperatureByStationMapJob",
        "meteo.TemperatureByStationJob"
      )

      case "meteoRecordByTemperature"        => (
        "meteo.RecordByTemperatureMapJob",
        "meteo.RecordByTemperatureReduceJob"
      )

      case "meteoRecordByTemperatureStation" => (
        "meteo.RecordByTemperatureStationMapJob",
        "meteo.RecordByTemperatureStationReduceJob"
      )

      case "meteoRecordByYearStation" => (
        "meteo.RecordByYearStationMapJob",
        "meteo.RecordByYearStationReduceJob"
      )

      case "yahooCountByKeyword"             => (
        "yahoo.CountByKeywordMapJob",
        "yahoo.CountByKeywordReduceJob"
      )

      case "yahooCountByAccount"             => (
        "yahoo.CountByAccountMapJob",
        "yahoo.CountByAccountReduceJob"
      )

      case "musicMeanBySong"                 => (
        "music.MeanBySongMapJob",
        "music.MeanBySongReduceJob"
      )

      case "musicCountGrade"                 => (
        "music.CountGradeMapJob",
        "music.CountGradeReduceJob"
      )

      case "revenueByArticle"                => (
        "sales.RevenueByArticleMapJob",
        "sales.RevenueReduceJob"
      )

      case "revenueByShop"                   => (
        "sales.RevenueByShopMapJob",
        "sales.RevenueReduceJob"
      )

      case "revenueByShopAndArticle"         => (
        "sales.RevenueByShopAndArticleMapJob",
        "sales.RevenueReduceJob"
      )

      case "revenueByDate"                   => (
        "sales.RevenueByDateMapJob",
        "sales.RevenueReduceJob"
      )

      case "generated"                       => (
        "generated.GeneratedDataMapJob",
        "generated.GeneratedDataReduceJob"
      )
    })

}
