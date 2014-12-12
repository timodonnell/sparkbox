sparkbox
========

Commandline frontends to some [Apache Spark](http://spark.incubator.apache.org/)
[MLlib](https://spark.apache.org/mllib/MLlib) machine learning routines. Very little functionality currently.

BEWARE: this currently does not center and scale features when training, which
makes the SGD optimization perform very poorly. This is really not usable yet.

# Local example

Build:

```
mvn package
```

To see available commands, run:

```
scripts/sparkbox
```

For example:

```
scripts/sparkbox train-linear --input src/test/resources/example1.csv --out model.avro
```

then:

```
scripts/sparkbox predict-linear --input src/test/resources/example1.csv --model model.avro --out results.csv
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
