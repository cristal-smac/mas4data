import java.io._
val r = scala.util.Random

val path = "/mnt/Data/Artificial/"
val nbNodes = 8
val nbValuesPerKey = 1000
val nbTasks = 100000

val load = new Array[Int](nbNodes)
val buffer = new Array[PrintWriter](nbNodes)
for (mapper <- 0 until nbNodes){
  val numFile = mapper + 1
  val file = new File(path+"mapper"+numFile+".txt")
  file.createNewFile()
  val writer = new PrintWriter(file)
  buffer(mapper) = writer
}

for (i <- 1 to nbTasks){
  var mapper = i % nbNodes
  var idTask = i
  if (mapper == 0){
    mapper = r.nextInt(nbNodes-1) + 1
    idTask = mapper + (nbTasks+i) * nbNodes
  }
  for (j <- 1 to nbValuesPerKey) buffer(mapper).write(s"$idTask;1\n")
  load(idTask%nbNodes) += 1
}

for (mapper <- 0 until nbNodes){
  buffer(mapper).close()
  println(s"load($mapper)=${load(mapper)}")
}
