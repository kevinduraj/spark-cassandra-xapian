#!/bin/bash
#-----------------------------------------------------------------------------------#
  if [ "$1" == "0" ] 2>/dev/null; then
    echo "sbt clean && sbt package"
    sbt clean && sbt package
    #mvn clean install
    #mvn clean compile assembly:single

#-----------------------------------------------------------------------------------#
elif [ "$1" == "1" ] 2>/dev/null; then

spark-submit                                        \
  --class "YoutubeVideos"                           \
  --master local[16]                                \
  --driver-memory   32G                             \
  --executor-memory 16G                             \
  target/scala-2.11/spark-cassandra_2.11-1.0.jar    \
  "export_data"

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

