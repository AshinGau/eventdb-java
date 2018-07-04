#!/bin/sh
hadoop fs -rm /ws/coprocessor/eventdb-1.0.0.jar
hadoop fs -put target/eventdb-1.0.0.jar /ws/coprocessor/
java -jar target/eventdb-1.0.0.jar createTable $1 4
java -jar target/eventdb-1.0.0.jar observer $1 org.osv.eventdb.fits.FitsObserver /ws/coprocessor/eventdb-1.0.0.jar
java -jar target/eventdb-1.0.0.jar insertHeFits /home/ashin/ws/EventDB/hxmt/P010129900101-20170827-01-01/HXMT_P010129900101_HE-Evt_FFFFFF_V1_1RP.FITS $1