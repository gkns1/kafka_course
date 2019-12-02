# kafka_course

All code based on the course material with some modifications.
The project contains several tutorial classes as well as a custom kafka producer & consumer.

The config_dummy.properties have to be replaced with config.properties with the correct values for the code to work.

kafka-producer-twitter takes a real-time data stream and sends it to kafka. It's idempotent and uses snappy compression. 
linger and batch size can be set to higher values for bigger batches.

kafka-consumer-elasticsearch sends kafka data to elastisearch (built in mind with bonsai.io). 
Reads from the earliest offset and committs manually every 100 poll batch (configurable in the code). Idempotence based on tweet ID.
