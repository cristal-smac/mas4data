#!/bin/sh
exec scala "$0" "$@"
!#
object Check extends App {
  import java.io._

  val dir = new File(args(0))
  val files = dir.listFiles
  val chunks = files.map(_.getName).filter(_.startsWith("mapper"))
  val keyAndChunksNb = chunks groupBy {
    _.split("_")(1)
  } map {
    x => x._1 -> x._2.toList.length
  }
  val nbTasks = keyAndChunksNb.size
  val sortedNbChunks = keyAndChunksNb.values.toList.sorted

  println("Task number: " + nbTasks)

  println("Maximum number of chunks in a task: " + keyAndChunksNb.values.max)

  println(
    "Average number of chunks by task: " + keyAndChunksNb.values.sum / nbTasks
  )

  println(
    "Median number of chunks by task: " + sortedNbChunks(nbTasks / 2)
  )

  println(
    "Number of tasks with 1 chunk: " + sortedNbChunks.filter(_ == 1).length
  )

  {
    val sortedByNbOfChunks = keyAndChunksNb.toList.sortBy(_._2)
    val topTen = sortedByNbOfChunks.drop(nbTasks - 10)
    val lolTen = sortedByNbOfChunks.take(10)
    val formatedTopTen = topTen.map(x => "\t" + x) mkString "\n"
    val formatedLolTen = lolTen.map(x => "\t" + x) mkString "\n"

    println(
      "Ten tasks with the most chunks (key, nb chunks):\n" + formatedTopTen
    )
    println(
      "Ten tasks with the least chunks (key, nb chunks):\n" + formatedLolTen
    )
  }

}
