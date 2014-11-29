sparkbox
========

Commandline frontends to some [Apache Spark](http://spark.incubator.apache.org/)
[MLlib](https://spark.apache.org/mllib/MLlib) machine learning routines. Very little functionality currently.

# Local example

Build:

```
mvn package
```

Run:

```
scripts/sparkbox logistic-regression --input src/test/resources/example1.csv
```

# Cluster example

The `scripts/cluster-example` directory has a script to demonstrate how one
might run this on a cluster.

Users at Mount Sinai with access to the demeter cluster can invoke the example
script as written. For example:

```
SPARK_MASTER="yarn" \
SPARK_HOME=/hpc/users/ahujaa01/src/spark/dist/ \
	scripts/cluster-examples/sparkbox-hammerlab-demeter-cluster \
	logistic-regression \
		--input hdfs://demeter-nn1.demeter.hpc.mssm.edu/user/.../data.csv \
		--reg-param 5.0
```
