
# Kafka Connect connector for Amazon Kinesis 

<h2>
Overview
</h2>

This package offers both Source and Sink Connectors for the Amazon Kinesis
data system.  Because Kinesis expects ByteStream data, the data conversion 
between Kafka and Kinesis streams can be a bit tricky.   The default for the 
Sink class is to send the SinkRecord converted with the default valueConverter 
class.  The Source class will attempt to decode the bytestream using the
Connect Framework's value conversions.  Effectively, this allows Kinesis to 
be used as an intermediate step in a normal Kafka Connect pipeline.

<h2>
Kinesis Background
</h2>

A good overview of Kinesis streams can be found with the AWS documentation.
See http://docs.aws.amazon.com/streams/latest/dev/fundamental-stream.html for a 
tutorial on generating your own stream with some sample data.


# Notes on Streams and Topics

The Connector will not automatically create a Kinesis Stream for the Sink, so
you should use the AWS tools to properly create one before launching the
connector.   The Connector will create new Kafka topics as targets for the
Source connector if automatic topic creation is enabled on the Kafka brokers.

	The best practice is to create both Streams and Topics appropriatedly
	before launching the connectors.

# Running in development

```
mvn clean package
export CLASSPATH="$(find target/ -type f -name '*.jar'| grep '\-package' | tr '\n' ':')"
$CONFLUENT_HOME/bin/connect-standalone \
	$CONFLUENT_HOME/etc/schema-registry/connect-avro-standalone.properties \
	[sink | source] -quickstart.properties
```
A simple script that enables a loopback test is included with this package;
see standalone-test .

