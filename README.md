sparkbox
========

Commandline frontends to some [Apache Spark](http://spark.incubator.apache.org/)
[MLlib](https://spark.apache.org/mllib/MLlib) machine learning routines.

This is currently just a convenient place for me to put trivial tools and ad hoc analyses.

# Example

Build:

```
mvn package
```

Run:

```
scripts/sparkbox logistic-regression --input /tmp/big.csv
```
