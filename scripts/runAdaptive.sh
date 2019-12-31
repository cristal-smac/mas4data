rm logAdaptive.txt;
sbt "run mapreduce.adaptive.Monitor" 2>&1 | tee -a logAdaptive.txt
