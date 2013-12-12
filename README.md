# CassandraPerf

Some Groovy scripts to figure out the fastest way to get data from a Java application into Cassandra.

Download Groovy here: http://groovy.codehaus.org/Download

Its easy to install. Just unzip it and put the bin directory on your path.

## Running Tests

Each test is runnable out of the box on local Cassandra instance but should be run against a real cluster. To do
this create a file in this directory called config.groovy and list some nodes in your cluster:

    def nodes = ["10.0.0.10", "10.0.0.11", "10.0.0.12"]

You can also change other test parameters in the same way.
