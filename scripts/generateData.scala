import java.io._
/*
 * Generate data files
 */

val path = "/mnt/Data/Artificial/"
val nbNodes = 8
val nbValuesPerKey = 10000
val nbTasks = 10000

val buffer = new Array[PrintWriter](nbNodes)

for (mapper <- 0 until nbNodes){
  val numFile = mapper + 1
  val file = new File(path+"mapper"+numFile+".txt")
  file.createNewFile()
  val writer = new PrintWriter(file)
  buffer(mapper) = writer
}

for (idTask <- 1 to nbTasks){
  val reducer = idTask % nbNodes
  val mapper = reducer % nbNodes
  for (i <- 1 to nbValuesPerKey) buffer(mapper).write(s"$idTask;1\n")
}

for (mapper <- 0 until nbNodes){
  buffer(mapper).close()
}





