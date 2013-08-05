java $SBT_OPS -Xms1g -Xmx1g -server -XX:PermSize=128m -XX:MaxPermSize=256m -XX:+UseParallelOldGC -jar `dirname $0`/lib/sbt-launch.jar "$@"
