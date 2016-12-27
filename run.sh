#!/bin/bash
#-----------------------------------------------------------------------------------#
#  --master spark://69.13.39.34:7077                \
#  --master local[16]                               \
#  --driver-memory   32G                            \
#  --executor-memory 16G                            \
#-----------------------------------------------------------------------------------#
OUTPUT_PREFIX='/home/data/5'
#-----------------------------------------------------------------------------------#
  if [ "$1" == "0" ] 2>/dev/null; then
    echo "sbt clean && sbt package"
    sbt clean && sbt package

#-----------------------------------------------------------------------------------#
elif [ "$1" == "1" ] 2>/dev/null; then
rm -fR $OUTPUT_PREFIX 
mkdir -p $OUTPUT_PREFIX
echo "$(date +"%Y-%m-%d %H:%M") - SPARK JOB1 STARTED" > spark.log

time spark-submit                                   \
  --class "YoutubeVideos"                           \
  --master spark://69.13.39.34:7077                 \
  --driver-memory   64g                             \
  --executor-memory 16g                             \
  target/scala-2.11/spark-cassandra_2.11-1.0.jar    \
  "export_data"                                     \
  $OUTPUT_PREFIX 

echo "$(date +"%Y-%m-%d %H:%M") - SPARK JOB1 ENDED" >> spark.log
#-----------------------------------------------------------------------------------#
else

  echo "+--------------------------------------------------+"
  echo "| ./run.sh 0 -- sbt clean && sbt package           |"
  echo "+--------------------------------------------------+"
  echo "| ./run.sh 1 -- count_null                         |"
  echo "| ./run.sh 2 -- count_all                          |"
  echo "| ./run.sh 3 -- export_data                        |"
  echo "+--------------------------------------------------+"

fi
#-----------------------------------------------------------------------------------#

