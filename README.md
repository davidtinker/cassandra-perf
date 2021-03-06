# CassandraPerf

Some Groovy scripts to figure out the fastest way to get data from a Java application into Cassandra.

Download Groovy here: http://groovy.codehaus.org/Download

Its easy to install. Just unzip it and put the bin directory on your path.

## Running Tests

Each test is runnable out of the box on local Cassandra instance but should be run against a real cluster. To do
this create a file in this directory called config.properties and list some nodes in your cluster:

    nodes = 10.0.0.10, 10.0.0.11, 10.0.0.12

You can also change other test parameters in the same way.

Now you can run the tests:

    $ ./InsertBatchOfPreparedStatements.groovy

It will take a while for the first run to start as the DataStax Java driver and all its dependencies have to be
downloaded.

## Results

3 node cluster, each node:
Intel® Xeon® E3-1270 v3 Quadcore Haswell 32GB RAM, 1 x 2TB commit log disk, 2 x 4TB data disks (RAID0)

Using a batch of prepared statements is about 5% faster than inline parameters:

InsertBatchOfPreparedStatements:
Inserted 2551704 rows in 100000 batches using 256 concurrent operations in 15.785 secs, 161653 rows/s, 6335 batches/s

InsertInlineBatch:
Inserted 2551704 rows in 100000 batches using 256 concurrent operations in 16.712 secs, 152686 rows/s, 5983 batches/s

