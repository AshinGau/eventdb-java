#!/bin/sh
PRODIR=$(cd `dirname $0`; pwd)
TARGETDIR=$PRODIR/target
JARS=$TARGETDIR/eventdb-1.0.0.jar
for jar in $TARGETDIR/lib/*.jar
do
	JARS=$JARS:$jar
done
java -cp $JARS org.osv.eventdb.Run $@
