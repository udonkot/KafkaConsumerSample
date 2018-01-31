@echo off
setlocal

set ROOTDIR=.
set LIBPATH=%ROOTDIR%\lib\
set CLASSPATH=bin;%LIBPATH%slf4j-api-1.7.25.jar;%LIBPATH%logback-classic-1.2.3.jar;%LIBPATH%logback-core-1.2.3.jar;%LIBPATH%kafka-clients-1.0.0.jar;

cd %ROOTDIR%

echo "java -cp %CLASSPATH% com.example.kafka.consumer.ConsumerMain %1 -Dlogback.configurationFile=./logback.xml
java -cp %CLASSPATH% com.example.kafka.consumer.ConsumerMain %1 -Dlogback.configurationFile=./logback.xml

pause