#!/usr/bin/env bash
set -eo pipefail


current_dir=$(find . -type d -name "jkvs")

echo "here "
if [[ -n $current_dir   ]]; then
	echo "removing current jkvs directory and cloning new one with git"
	rm -rf ./jkvs

fi

which mvn
if [[ $? -ne 0 ]]; then
	echo "could not find mvn installed"
fi

which java
if [[ $? -ne 0 ]]; then
	echo "could not find java installed"
fi

git clone https://github.com/persona-mp3/jkvs.git 
cd ./jkvs/jkvs && mvn package -DskipTests && clear
echo "starting jkvs database on tcp:9090 for jkvs"
echo "To change the port, quit the process using Ctrl+C and then run the jar file 
			java -jar target/jkvs-server.javr --port <number> --addr <ip_addr>
			To change log levels,  edit the jkvs/jkvs/src/main/resources/log4j2.xml to use 
			debug, info, warn or fatal log levels
			"
java -jar target/jkvs-server.jar
