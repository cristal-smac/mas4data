package object utils {

  /** Round up a double value.
    *
    * @param x         value to round up
    * @param nbDecimal number of decimal to keep in the result
    * @return the rounded decimal
    */
  def roundUpDouble(x: Double, nbDecimal: Int): Double =
    math.BigDecimal(x).setScale(
      nbDecimal,
      math.BigDecimal.RoundingMode.HALF_UP
    ).toDouble

}
