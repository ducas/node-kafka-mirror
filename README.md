# Kafka Mirror

This project allows you to mirror a Kafka topic to another topic. It simply creates a high-level consumer that listens to one topic and a high-level producer that publishes to another.

## Getting Started

Once you've cloned the repo and run *npm install*, you can run mirror with the following arguments:

* from\_zk\_connect - the Zookeeper connection string used for the Kafka source cluster
* from\_topic - the Kafka source topic to mirror from
* to\_zk\_connect - the Zookeeper connection string used for the Kafka destination cluster
* to_topic - the Kafka destination topic to mirror to
* clientId - the Client ID and consumer group name to use when connecting to Kafka/Zookeeper

E.g.

    node app.js --from_zk_connect=source.zk.local:2181 --from_topic=source.topic --to_zk_connect=dest.zk.local:2181 --to_topic=dest.topic

This will -
* connect to Zookeeper at *source.zk.local* on port 2181 to find the appropriate Kafka cluster nodes to use as its source
* set up a high-level consumer listening to messages published to *source.topic*
* connect to Zookeeper at *dest.zk.local* on port 2181 to find the appropriate Kafka cluster nodes to use as its destination
* set up a high-level produces that publishes messages to *dest.topic*
