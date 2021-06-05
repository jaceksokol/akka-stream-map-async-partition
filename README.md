# akka-stream-map-async-partition
Simple operator for `akka-stream` that allows to process elements in a parallel but taking into consideration possible partition of an element.
Thanks to such an approach you can maximise throughput i.e. when processing messages from Kafka.

## Usage
```scala
libraryDependencies ++= Seq(
  "com.github.jaceksokol" %% "akka-stream-map-async-partition" % "VERSION"
)
```
