#!/bin/sh
java -server -Xms512m -Xmx2048m -XX:+UseG1GC -XX:+UseCompressedOOps -XX:+UseTLAB -Dlog4j.configurationFile=log4j2.xml -Djava.system.class.loader=ua.com.fielden.platform.classloader.TgSystemClassLoader -cp .:lib/* helsinki.webapp.StartOverHttp application.properties
