#!/usr/bin/env groovy

// This test inserts batches using a BatchStatement filled with BoundStatement's

import com.datastax.driver.core.BatchStatement
@Grab('com.datastax.cassandra:cassandra-driver-core:2.0.0-rc1')
@Grab('org.xerial.snappy:snappy-java:1.1.1-M1')
@Grab('net.jpountz.lz4:lz4:1.2.0')
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Host
import com.datastax.driver.core.Metadata
import com.datastax.driver.core.ResultSet
import com.google.common.util.concurrent.FutureCallback
import com.google.common.util.concurrent.Futures

import java.util.concurrent.CountDownLatch
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicInteger

// override these values by creating config.properties in this directory

def nodes = ["127.0.0.1"]       // these tests really need to run against a remote cluster to be any good
def keyspace = "perf_test"
def iterations = 100000
def warmup = 10000
def batchSize = 50
def concurrency = 256

File cfg = new File("config.properties")
if (cfg.exists()) {
    Properties p = new Properties()
    p.load(new ByteArrayInputStream(cfg.bytes))
    if (p.nodes) nodes = ((String)p.nodes).split('[\\s]*,[\\s]*') as List
    if (p.keyspace) keyspace = p.keyspace
    if (p.iterations) iterations = p.iterations as Integer
    if (p.warmup) warmup = p.warmup as Integer
    if (p.batchSize) batchSize = p.batchSize as Integer
    if (p.concurrency) concurrency = p.concurrency as Integer
}

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
    CountDownLatch latch = new CountDownLatch(n)
    Semaphore available = new Semaphore(concurrency)
    AtomicInteger totalRows = new AtomicInteger()
    def ps = session.prepare("insert into ${keyspace}.wibble (id, name, info) values (?, ?, ?)")
    for (int i = 0; i < n; i++) {
        BatchStatement bs = new BatchStatement(BatchStatement.Type.UNLOGGED)
        def rows = rnd.nextInt(batchSize) + 1
        for (int j = 0; j < rows; j++) {
            bs.add(ps.bind(Integer.toString(rnd.nextInt(10000)), "name" + rnd.nextInt(10), "info" + rnd.nextInt(1000)))
        }
        available.acquire()
        def f = session.executeAsync(bs)
        Futures.addCallback(f, new FutureCallback<ResultSet>() {
            public void onSuccess(ResultSet rs) {
                latch.countDown()
                totalRows.addAndGet(rows)
                available.release()
            }

            public void onFailure(Throwable t) {
                latch.countDown()
                t.printStackTrace()
                available.release()
            }
        });
    }
    latch.await()
    return totalRows
}

println("Inserting ${warmup} warmup batches")

test(warmup)

println("Inserting ${iterations} test batches")

long start = System.currentTimeMillis()
int rows = test(iterations).intValue()
def s = (System.currentTimeMillis() - start) / 1000
println "Inserted ${rows} rows in ${iterations} batches using ${concurrency} concurrent operations in ${s} secs, " +
        "${(int)(rows/s)} rows/s, ${(int)(iterations/s)} batches/s"

session.shutdown().get()
cluster.shutdown().get()
