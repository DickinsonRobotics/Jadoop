# Jadoop
**Java Grid Computing Framework for use with Hadoop Clusters**

Jadoop is a Java API to facilitate the use of a hadoop cluster for
grid computing applications. Jadoop makes it easy to create a Java 
application that: specifies a list of command line instructions, 
executes them in parallel on a hadoop cluster, and collects the 
output (stdout & stderr) generated by the execution of the instructions.

Of course hadoop is much more than this and there are many special purpose 
grid computing solutions available. Thus, Jadoop may not be the right solution 
for your application. But if you have access to an existing hadoop cluster 
and need to run a grid computing application from Java, then Jadoop may be 
a solution to consider.

Jadoop is free software: you can redistribute it and/or modify it under the 
terms of the GNU General Public License as published by the Free Software 
Foundation, either version 3 of the License, or (at your option) any later 
version.

Jadoop is distributed in the hope that it will be useful, but WITHOUT ANY 
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR 
A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 
You should have received a copy of the GNU General Public License along with 
Jadoop. If not, see <http://www.gnu.org/licenses/>.

#The Jadoop Distribution:

The Jadoop Distribution can be obtained from:

Downloading and unzipping JadoopDist.zip will produce the directory structure:
```
  JadoopDist
   +- doc						* JavaDoc for the Jadoop core classes
   |  +- ...
   |  +- index.html				* Home page for the JavaDoc.
   |  +- ...
   +- examples					* Source and .class files for the sample code
   |  +- ...
   +- jadoop.jar				* All of the Jadoop classes in a jar.
   +- JadoopCode.zip			* All of the source necessary to produce a distriubtion.
   +- LICENSE.txt				* GNU GPL v3
   +- readme.txt				* This file.
```

#Running a Jadoop Example Program:

The fastest and easiest way to run a Jadoop example program is:

1. Download a hadoop distribution from http://hadoop.apache.org/releases.html
   Jadoop has been tested with hadoop-2.7.2, but should work with 2.6.x and later 
   releases as well.
2. Open a terminal window with a bash shell, change to the hadoop distribution 
   directory (the one containing the bin, share and etc (and other) directories).  
   Set the following environment variables:
    ```
    export HADOOP_ROOT=$(pwd)
    export HADOOP_CONFIG=$HADOOP_ROOT/etc/hadoop
    export HADOOP_JARS=$HADOOP_ROOT/share/hadoop
     
    NOTE: If not using a bash shell, the syntax for setting the environment variable
    may vary slightly. 
    ```
3. If you have a hadoop cluster already running, you may need to modify
   the HADOOP_CONFIG set above so that it points to the etc/hadoop directory 
   containing your configuration.  Then skip to step 5.
4. If you do not have a hadoop cluster running, open a second terminal window
   with a bash shell.  Repeat step 2 in this window.  Then launch the hadoop 
   CLI Minicluster using the following commands:
    ```
    export HADOOP_CLASSPATH=$HADOOP_JARS/yarn/test/hadoop-yarn-server-tests-2.7.1-tests.jar
	$HADOOP_ROOT/bin/hadoop jar $HADOOP_JARS/mapreduce/hadoop-mapreduce-client-jobclient-2.7.1-tests.jar minicluster
    ```
5. From the terminal used in step 2, change into the JadoopDist directory and run 
   the example:
    ```
    export HADOOP_CLASSPATH=$HADOOP_CONFIG:$HADOOP_JARS/common/*:$HADOOP_JARS/common/lib/*:$HADOOP_JARS/hdfs/*:$HADOOP_JARS/mapreduce/*:$HADOOP_JARS/yarn/*
    java -cp .:examples:$HADOOP_CLASSPATH:jadoop.jar Hostnames
    ```
6. The output should terminate with a list of the 10 hostnames on which the tasks
   were run.  If the CLI Minicluster was used, this will be 10 copies of the name
   of your machine, otherwise it will be the names of 10 of the machines in the
   hadoop cluster.
7. All examples in the JadoopDist/examples directory can be run in a similar
   manner:
  * Hostnames - runs the unix hostname command on each node.
  * Easter - runs the unix ncal -e command, with a different command  
             line argument indicating the year, on each node.
  * CoinFlipExperiemnt - runs a custom Java program (CoinFlipTask),
     	                 with a command line parameter, on each node
8. Additional details on how Jadoop works can be found in the java documentation
   (JadoopDist/doc/index.html) and the source (unzip JadoopDist/JadoopCode.zip).
   
#Modifying the Jadoop Source:

Create an empty Jadoop directory and expand the JadoopCode.zip file inside 
the empty Jadoop directory.  This will result in the following directory 
structure:
```
Jadoop
 +- JadoopCode (expanded from JadoopCode.zip)
     +- src
     |  +- jadoop
     |     +- ... java files ...
     |     +- util
     |        +- ... java files ...
     +- test
     |  +- jadoop
     |     +- ... java files ...
     +- examples
	 |  +- ... java files ...
     |  
     +- build.xml
     +- build.properties
     +- readme.txt
     +- LICENSE.txt 
```
 
The JadoopCode directory can be used as the base directory for a project in an 
IDE (e.g eclipse).  The src, test and examples directories should be added as 
source directories in the project. The src directory tree contains all of the core 
functionality of Jadoop.  The test directory contains a collection of JUnit 4 
tests for the core Jadoop functionality.  The examples directory contains the source
code for for a few illustrative examples of Jadoop use.

A large number of jar files from the hadoop distribution will need to be added to
the buildpath for your project to get the source and tests to compile and run.  
These include at least the following:
```	
hadoop-2.7.1	(or your version)
 +- share
	+- hadoop
	   +- common
	   |  +- *.jar			
	   |  +- lib
	   |     +- *.jar		
	   +- hdfs
	   |  +- *.jar
	   +- mapreduce
	   |  +- *.jar
	   |  +- lib
	   |  |  +- *.jar
	   |  +- lib-examples
	   |     +- *.jar
	   +- yarn
	      +- *.jar
	      +- lib
	      |  +- *.jar
          +- test
             +- *.jar
```

#Building a Jadoop Distribution:

To build a Jadoop distribution:

1. Edit the build.properties files so that 
  * hadoop.version matches your version of hadoop.
  * hadoop.home points to the root directory your hadoop installation.
2. In a terminal, change to the Jadoop/JadoopCode directory.
3. In the terminal, execute: ant dist 

The ant dist task will generate two directories (build and JadoopDist) and one
zip (JadoopDist.zip) within the Jadoop directory.  The build directory are the
artifacts used to create the contents of the JadoopDist directory, which will
have the same structure as the original one that you downloaded.  The JadoopDist.zip
file is simply a zipped version of the JadoopDist directory and its contents.  

#Jadoop Credits:

The initial implementation of Jadoop was written at Dickinson College by Grant Braught,
Xiang Wei & Michael Skalak. Partial support for the initial development of Jadoop was 
provided by a grant for "Tool Development for Research in Evolutionary and Developmental 
Robotics" from the Dickinson College STEP Program. Summer, 2015.                