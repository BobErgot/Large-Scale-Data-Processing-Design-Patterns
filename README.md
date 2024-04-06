# Large Scale Data Processing Design Patterns

## Overview

As a passionate reader and implementer of the "MapReduce Design Patterns" book, I've created this repository to host implementations of the design patterns described therein. These patterns are essential for solving specific big data processing problems using the MapReduce framework. This repository serves as a practical resource for developers looking to apply these patterns in their projects, featuring a wide array of examples that demonstrate the utility and application of each design pattern in real-world data processing tasks.

### Design Patterns Covered

- **Summarization Patterns**
	- Counting and Summing
	- Min/Max Statistics
	- Average
	- Median and Percentiles
	- Histograms
- **Filtering Patterns**
	- Filtering
	- Top Ten
	- Unique
	- Bloom Filter
- **Data Organization Patterns**
	- Structured to Hierarchical
	- Partitioning
	- Binning
	- Total Order Sorting
- **Join Patterns**
	- Reduce Side Join
	- Map Side Join
	- Composite Join
- **Metapatterns**
	- Job Chaining
	- Input and Output Format
- **Input and Output Patterns**
	- Custom Input and Output Format
	- Multiple Inputs and Outputs

## Repository Structure
- **Input Folder**:
	- Location: `input/`
	- Description: Contains various inputs needed for the code execution of different patterns. This ensures users have all necessary data and parameters to effectively run and test the examples provided within each design pattern directory.

- **Results Folder**:
	- Location: `results/`
	- Description: For storing output data and execution results, the repository features a `results` folder. Subdirectories within this folder are named identically to the source (`src`) directories corresponding to each design pattern. This setup facilitates easy navigation and correlation between the executed design pattern examples and their respective outcomes.

## Installation

These components need to be installed first:
- OpenJDK 11
- Hadoop 3.3.5
- Maven (Tested with version 3.6.3)
- AWS CLI (Tested with version 1.22.34)

After downloading the hadoop installation, move it to an appropriate directory:

`mv hadoop-3.3.5 /usr/local/hadoop-3.3.5`

## Environment

1) Example ~/.bash_aliases:
   ```
   export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
   export HADOOP_HOME=/usr/local/hadoop-3.3.5
   export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
   export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
   ```

2) Explicitly set `JAVA_HOME` in `$HADOOP_HOME/etc/hadoop/hadoop-env.sh`:

   `export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64`

## Execution

All the build & execution commands are organized in the Makefile.
1) Unzip project file.
2) Open command prompt.
3) Navigate to directory where project files unzipped.
4) Edit the Makefile to customize the environment at the top.
   Sufficient for standalone: hadoop.root, jar.name, local.input
   Other defaults acceptable for running standalone.
5) Standalone Hadoop:
	- `make switch-standalone`		-- set standalone Hadoop environment (execute once)
	- `make local`
6) Pseudo-Distributed Hadoop: (https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation)
	- `make switch-pseudo`			-- set pseudo-clustered Hadoop environment (execute once)
	- `make pseudo`					-- first execution
	- `make pseudoq`				-- later executions since namenode and datanode already running
7) AWS EMR Hadoop: (you must configure the emr.* config parameters at top of Makefile)
	- `make make-bucket`			-- only before first execution
	- `make upload-input-aws`		-- only before first execution
	- `make aws`					-- check for successful execution with web interface (aws.amazon.com)
	- `download-output-aws`		-- after successful execution & termination

## Resources

To delve deeper into MapReduce design patterns and their application in big data processing, consider exploring the following resources:

- "MapReduce Design Patterns" by Donald Miner and Adam Shook
- [Apache Hadoop Documentation](https://hadoop.apache.org/docs/current/)