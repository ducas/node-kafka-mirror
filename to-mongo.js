var kafka = require('kafka-node'),
    MongoClient = require('mongodb'),
    Promise = require('promise'),
    args = require('yargs').argv,
    bunyan = require('bunyan'),

    clientId = args.clientId || 'kafka-mongo-mirror',
    logLevel = args.logLevel || 'info',
    log = bunyan.createLogger({ name: clientId, level: logLevel }),

    fromClient = new kafka.Client(args.from_zk_connect, clientId);

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
    fetchMaxBytes: 1024 * 1024 * 10, // 10MB
    // If set true, consumer will fetch message from the given offset in the payloads 
    fromOffset: false,
    // If set to 'buffer', values will be returned as raw buffer objects. 
    encoding: 'utf8'
});

var db;

var exit = function () {
    consumer.close(function () {
        var waitToExit = function () {
            log.debug('Waiting for queue to drain...')
            if (queue.length == 0) {
                return db.close(function () {
                    clearInterval(monitorInterval);
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

        log.debug({ items: items }, 'Publishing to destination collection...');

        db.collection(args.to_collection).insertMany(
            items.map(JSON.parse),
            function (e, r) {
                if (e) {
                    log.error('Error sending message to destination: ' + e);
                    exit();
                }
                
                log.trace('Publish successful.');
                queue = queue.slice(items.length);

                if (queue.length == 0) {
                    consumer.commit(true, function (err, data) {
                        if (err) {
                            log.error('Failed to commit offset.');
                            return exit();
                        }

                        log.trace('Commit succesful.');
                        // only set the timeout again if we haven't received the stop signal
                        // OR if we have received it and there is still stuff in the queue.
                        if (!stop || queue.length > 0) setTimeout(publish, publishDelay);
                    });
                }
                else setTimeout(publish, publishDelay)
            });

    },
    monitor = function () {
        log.debug({queueLength: queue.length});
    },
    monitorInterval = setInterval(monitor, 500);


MongoClient.connect(args.to_mongo_connect, function (err, d) {
    db = d;
    if (!db.collections(args.to_collection)) {
        log.info('Creating collection ' + args.to_collection);
        db.createCollection(args.to_collection, function (e) {
            if (e) {
                log.error(e, 'Error creating collection.');
                exit();
            }
            setTimeout(publish, publishDelay);
        });
    }
    else {
        log.trace('Collection ' + args.to_collection + ' exists, start publishing.');
        setTimeout(publish, publishDelay);
    }
});

consumer.on('message', function (m) {
    queue.push(m.value);
});

consumer.on('error', function (e) {
    log.error(e, 'Error receiving messages from source.');
    exit();
});

consumer.on('offsetOutOfRange', function (e) {
    log.warn(e, 'Error receiving messages from source.');
});

process.on('SIGINT', function () {
    log.info('Shutting down...');
    exit();
});

