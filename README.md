# Kafka Mirror

This project allows you to mirror a Kafka topic to somewhere else. It simply creates a high-level consumer that listens to one topic and pushes the messages it receives to another data store. It supports mirroring to:

* Kafka
* MongoDB

## Getting Started

Fist, you'll need to clone the repo and run *npm install*.

### Mirroring to another Kafka Topic

Specify the following arguments:

* from\_zk\_connect - the Zookeeper connection string used for the Kafka source cluster
* from\_topic - the Kafka source topic to mirror from
* to\_zk\_connect - the Zookeeper connection string used for the Kafka destination cluster
* to\_topic - the Kafka destination topic to mirror to
* clientId - the Client ID and consumer group name to use when connecting to Kafka/Zookeeper

E.g.

    node app.js --from_zk_connect=source.zk.local:2181 --from_topic=source.topic --to_zk_connect=dest.zk.local:2181 --to_topic=dest.topic

This will -
* connect to Zookeeper at *source.zk.local* on port 2181 to find the appropriate Kafka cluster nodes to use as its source
* set up a high-level consumer listening to messages published to *source.topic*
* connect to Zookeeper at *dest.zk.local* on port 2181 to find the appropriate Kafka cluster nodes to use as its destination
* set up a high-level produces that publishes messages to *dest.topic*

### Mirroring to a MongoDB collection

Specify the following arguments:

* from\_zk\_connect - the Zookeeper connection string used for the Kafka source cluster
* from\_topic - the Kafka source topic to mirror from
* to\_mongo\_connect - the MongoDB URL - e.g. mongodb://localhost:27017/mirror-db
* to\_collection - the destination collection to mirror to
* clientId - the Client ID and consumer group name to use when connecting to Kafka/Zookeeper

E.g.

    node to-mongo.js --from_zk_connect=source.zk.local:2181 --from_topic=topic-name --to_mongo_connect=mongodb://localhost:27017/mirror-db --to_topic=topic-name

This will -
* connect to Zookeeper at *source.zk.local* on port 2181 to find the appropriate Kafka cluster nodes to use as its source
* set up a high-level consumer listening to messages published to *source.topic*
* connect to MongoDB at *localhost* on port 27017 to find the *mirror-db* database
* create a collection called *topic-name* if it doesn't already exist
* use bulk update operations to push messages received from Kafka to the *topic-name* collection as they are received
