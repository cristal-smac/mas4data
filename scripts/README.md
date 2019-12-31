# Script descriptions

- analyseTasks.sh: print taskset profile (task number, average number of chunks
  by task, etc.) from the output mapper files.

- dataexp.sh: given a set of experiments folder, print the average value for
  each data tracked by the monitor (initiated CFP, canceled CFP, fairness,
  etc.).

- fairness.sh: from two contributions file (one of a classical run, one of a
  adaptive run), compare the two runs with contributions and fairness.

- runAdaptive.sh: launch an adaptive run.

- runClassic.sh: launch a classic run (i.e. non nego).

- sameResults.sh: from two results file, check if they contains the same tasks
  and if the result is the same for each task.

