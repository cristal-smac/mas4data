package utils.jobs.sales

import utils.jobs.MapJob

/** Map job for the count key problem. */
class RevenueByDateMapJob extends MapJob {

  type K = String

  type V = Double

  protected def map(line: String): Iterator[(String, Double)] = {
    val splitLine = line split ";"
    val key = if (splitLine(2).isEmpty) "empty key" else splitLine(2)
    val value =
      if (splitLine(5) != "montant") Some(splitLine(5).toDouble) else None

    if (value.isDefined) Iterator((key, value.get)) else Iterator()
  }

}
