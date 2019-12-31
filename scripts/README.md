# Script descriptions

- analyseTasks.sh: print taskset profile (task number, average number
  of chunks by task, etc.) from the output mapper files.

- dataexp.sh: given a set of experiments folder, print the average
  value for each data tracked by the monitor (initiated CFP, canceled
  CFP, fairness, etc.).

- fairness.sh: from two contributions file (one of a classical run,
  one of a adaptive run), compare the two runs with contributions and
  fairness.

- generateData.py: generate a dataset such that none task is assigned
to the first reducer while the tasks are assigned to the others
reducers which have similar workloads

- generateData.scala: generate a dataset such that most of the data
required for a task are not located on the same node as the assigned
reducer

- killCluster.sh: kill the daemons on the remote nodes

- launchCluster.sh: run the daemons on the remote nodes

- openCluster.sh: open all the ports on the nodes

- openNode.sh: opent all the the ports on the node

- runAdaptive.sh: launch an adaptive run.

- runClassic.sh: launch a classic run (i.e. non nego).

- sameResults.sh: from two results file, check if they contains the same tasks
  and if the result is the same for each task.

- shutdownCPU.sh: only one CPU is activated

- startCPU.sh: all the CPUs are activated 