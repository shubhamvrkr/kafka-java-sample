# Kafka Java SDK Example
This project demonstrate usage of kafka java SDK (`i.e kafka-clients ver:2.4.0`).

<b>Note:</b> Follow first 2 steps from [kafka documentation](https://kafka.apache.org/quickstart) to start zookeper and kafka instances.  

### Project demonstrate
 - Creation of topic with initial partition programatically
 - Creation of additional partitions to existing topic programatically
 - Custom partioner to distribute data to partiton based on data attributes
 - Custom serializer and de-serializer to serialize/deserialize object.
