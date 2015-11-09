var kafka = require('kafka-node'),
    Promise = require('promise'),
    args = require('yargs').argv,

    clientId = args.clientId || 'kafka-mirror';

var fromClient = new kafka.Client(args.from_zk_connect, clientId);

var consumer = new kafka.HighLevelConsumer(
    fromClient, [{
        topic: args.from_topic
    }], {
    //consumer group id
    groupId: clientId,
    // Auto commit config 
    autoCommit: true,
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

var exit = function () {
    consumer.close(true, function () {
        process.exit();
    });
}

var queue = [],
    stop = false,
    publish = function () {
        var items = queue.slice(0);
        queue = queue.slice(items.length);

        if (!stop && items.length == 0) return setTimeout(publish, 100);

        process.stdout.write(items.join(','));

        if (queue.length == 0) consumer.commit(function () {});

        if (!stop || queue.length > 0) setTimeout(publish, 100);

    };

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