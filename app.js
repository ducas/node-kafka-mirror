var kafka = require('kafka-node'),
    Promise = require('promise'),
    args = require('yargs').argv,
    bunyan = require('bunyan'),

    clientId = args.clientId || 'kafka-mirror',
    logLevel = args.logLevel || 'info',
    log = bunyan.createLogger({ name: clientId, level: logLevel }),

    fromClient = new kafka.Client(args.from_zk_connect, clientId),
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
    consumer.close(function () {
        var waitToExit = function () {
            log.debug('Waiting for queue to drain...')
            if (queue.length == 0) {
                return producer.close(function () {
                    process.exit();
                });
            }
            setTimeout(waitToExit, publishDelay);
        };
        waitToExit();
    });
};

var queue = [],
    stop = false,
    publishDelay = 100,
    publish = function () {
        if (!stop && queue.length == 0) {
            log.trace('Nothing to publish.');
            return setTimeout(publish, publishDelay);
        }

        var items = queue.slice(0);
        queue.slice(items.length);

        log.trace({ items: items }, 'Publishing to destination topic...');

        producer.send(
            [{
                topic: args.to_topic,
                messages: items,
                attributes: 2
            }],
            function (e) {
                if (e) {
                    log.error('Error sending message to destination: ' + e);
                    exit();
                }
                
                log.trace('Publish successful.');
                if (queue.length == 0) consumer.commit(function () { });

                if (!stop || queue.length > 0) setTimeout(publish, publishDelay);
            });

    };

producer.on('ready', function () {
    producer.createTopics([args.to_topic], false, function () {});
    setTimeout(publish, publishDelay);
});

consumer.on('message', function (m) {
    queue.push(m.value);
});

consumer.on('error', function (e) {
    log.error('Error receiving messages from source: ' + e);
    exit();
});

consumer.on('offsetOutOfRange', function (e) {
    log.error('Error receiving messages from source: ' + e);
    exit();
});

process.on('SIGINT', function () {
    log.info('Shutting down...');
    exit();
});

