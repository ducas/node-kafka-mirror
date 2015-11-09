var kafka = require('kafka-node'),
    Promise = require('promise'),
    args = require('yargs').argv,

    clientId = args.clientId || 'kafka-mirror';

var fromClient = new kafka.Client(args.from_zk_connect, clientId),
    toClient = new kafka.Client(args.to_zk_connect, clientId);

var consumer = new kafka.HighLevelConsumer(
    fromClient, [{
        topic: args.from_topic
    }], {
    //consumer group id
    groupId: clientId,
    // Auto commit config 
    autoCommit: false,
    autoCommitIntervalMs: 5000,
    // The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available at the time the request is issued, default 100ms 
    fetchMaxWaitMs: 100,
    // This is the minimum number of bytes of messages that must be available to give a response, default 1 byte 
    fetchMinBytes: 1,
    // The maximum bytes to include in the message set for this partition. This helps bound the size of the response. 
    fetchMaxBytes: 1024 * 10,
    // If set true, consumer will fetch message from the given offset in the payloads 
    fromOffset: false,
    // If set to 'buffer', values will be returned as raw buffer objects. 
    encoding: 'utf8'
});

var producer = new kafka.HighLevelProducer(toClient, { requireAcks: 1, ackTimeoutMs: 100 });

var exit = function () {
    consumer.close(true, function () {
        var waitToExit = function () {
            if (queue.length == 0) {
                return producer.close(function () {
                    process.exit();
                });
            }
            setTimeout(waitToExit, 100);
        };
        waitToExit();
    });
};

var queue = [],
    stop = false,
    publishTimeout,
    publish = function () {
        var items = queue.slice(0);
        queue = queue.slice(items.length);

        if (!stop && items.length == 0) return setTimeout(publish, 100);

        process.stdout.write(items.join(','));
        producer.send(
            [{
                topic: args.to_topic,
                messages: items,
                attributes: 2
            }],
            function (e) {
                if (e) {
                    console.error("Error sending message to destination: " + e);
                    exit();
                }
                
                if (queue.length == 0) consumer.commit(function () {});

                process.stdout.write('.');

                if (!stop || queue.length > 0) setTimeout(publish, 100);
            });

    };

producer.on('ready', function () {
    producer.createTopics([args.to_topic], false, function () {});
    setTimeout(publish, 100);
});

consumer.on('message', function (m) {
    queue.push(m.value);
});

consumer.on('error', function (e) {
    console.error("Error receiving messages from source: " + e);
    exit();
});

consumer.on('offsetOutOfRange', function (e) {
    console.error("Error receiving messages from source: " + e);
    exit();
});

process.on('SIGINT', function () {
    console.log("Shutting down...");
    exit();
});