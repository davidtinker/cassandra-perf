#!/usr/bin/env groovy

// This tests inserts batches of rows using a single CQL string with parameters included inline

@Grab('com.datastax.cassandra:cassandra-driver-core:2.0.0-rc1')
@Grab('org.xerial.snappy:snappy-java:1.1.1-M1')
@Grab('net.jpountz.lz4:lz4:1.2.0')
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Host
import com.datastax.driver.core.Metadata

// override these values by creating config.groovy in this directory

def nodes = ["127.0.0.1"]       // these tests really need to run against a remote cluster to be any good
def keyspace = "perf_test"
def iterations = 1000
def warmup = 100
def batchSize = 50

def cfg = new File("config.groovy")
if (cfg.exists()) evaluate(cfg)

Cluster cluster = Cluster.builder().addContactPoints(nodes as String[]).build();
Metadata metadata = cluster.getMetadata();
println "Connected to " + metadata.getClusterName()
for (Host host: metadata.getAllHosts()) {
    println("DC " + host.getDatacenter() + " host " + host.getAddress() + " rack " + host.getRack())
}

def session = cluster.connect()

session.execute("""
CREATE KEYSPACE IF NOT EXISTS ${keyspace}
WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3}
AND DURABLE_WRITES = true""")

session.execute("""
CREATE TABLE IF NOT EXISTS ${keyspace}.wibble (
  id text,
  name text,
  info text,
  PRIMARY KEY (id, name)
)
""")

def rnd = new Random(123)

def test = { int n ->
    int totalRows = 0
    for (int i = 0; i < iterations + warmup; i++) {
        StringBuilder b = new StringBuilder()
        b.append("BEGIN UNLOGGED BATCH\n")
        def rows = rnd.nextInt(batchSize) + 1
        for (int j = 0; j < rows; j++) {
            b.append("insert into ").append(keyspace).append(".wibble (id, name, info) values ('")
                    .append(rnd.nextInt(10000)).append("','name").append(rnd.nextInt(10)).append("','info")
                    .append(rnd.nextInt(1000)).append("')\n")
        }
        b.append("APPLY BATCH\n")
        session.execute(b.toString())
        totalRows += rows
    }
    return totalRows
}

println("Inserting ${warmup} warmup batches")

test(warmup)

println("Inserting ${iterations} test batches")

long start = System.currentTimeMillis()
int rows = test(iterations)
def s = (System.currentTimeMillis() - start) / 1000
println "Inserted ${rows} rows in ${iterations} batches in ${s} secs, " +
        "${(int)(rows/s)} rows/s, ${(int)(iterations/s)} batches/s"

session.shutdown().get()
cluster.shutdown().get()
