rm logClassic.txt ;
sbt "run mapreduce.classic.Supervisor" 2>&1 | tee -a logClassic.txt
