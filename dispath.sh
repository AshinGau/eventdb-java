#!/bin/sh
scp eventdb root@sbd02:~/eventdb/eventdb-java/
scp eventdb root@sbd03:~/eventdb/eventdb-java/
scp eventdb root@sbd04:~/eventdb/eventdb-java/
scp -r target root@sbd02:~/eventdb/eventdb-java/
scp -r target root@sbd03:~/eventdb/eventdb-java/
scp -r target root@sbd04:~/eventdb/eventdb-java/
scp config.properties root@sbd02:~/eventdb/eventdb-java/
scp config.properties root@sbd03:~/eventdb/eventdb-java/
scp config.properties root@sbd04:~/eventdb/eventdb-java/
