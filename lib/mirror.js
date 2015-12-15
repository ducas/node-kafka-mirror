var kafka = require('kafka-node'),
    Promise = require('promise'),
    bunyan = require('bunyan');

module.exports = function (options) {

    var clientId = options.clientId || 'kafka-mirror',
        log = options.log,
        fetchMaxBytes = options.fetchMaxBytes || 10 * 1024 * 1024, // 10MB
        zk_connect = options.args.from_zk_connect,
        topic = options.args.from_topic,
        producer = options.producer,
        pauseOnSend = options.pauseOnSend || false;

    if (!zk_connect) {
        log.error('Argument from_zk_connect not specified.');
        process.exit(1);
    }
    if (!topic) {
        log.error('Argument from_topic not specified.');
        process.exit(1);
    }

    var fromClient = new kafka.Client(zk_connect, clientId),
        consumer = new kafka.HighLevelConsumer(
            fromClient, [{
                topic: topic
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
            fetchMaxBytes: fetchMaxBytes, 
            // If set true, consumer will fetch message from the given offset in the payloads 
            fromOffset: false,
            // If set to 'buffer', values will be returned as raw buffer objects. 
            encoding: 'utf8'
        });

    var exit = function (code) {
        stop = true;
        consumer.pause();
        var waitToExit = function () {
            log.trace('Waiting for queue to drain...')
            if (stopped) {
                consumer.close(function () {
                    producer.close(function () {
                        clearInterval(monitorInterval);
                        process.exit(code);
                    });
                });
            }
            else {
                setTimeout(waitToExit, publishDelay);
            }
        };
        waitToExit();
    };

    var queue = [],
        stop = false,
        stopped = false,
        publishDelay = 100,
        mirroredMessages = 0,
        publish = function () {

            if (!stop && queue.length == 0) {
                log.trace('Nothing to publish.');
                return setTimeout(publish, publishDelay);
            }
            else if (stop && queue.length == 0) {
                return stopped = true;
            }

            if (pauseOnSend) {
                log.trace('Pausing consumer...');
                consumer.pause();
            }

            var items = queue.slice(0);

            log.trace({ items: items }, 'Publishing to destination collection...');

            producer.send(
                items,
                function (e, r) {
                    if (e) {
                        log.error(e, 'Error sending message to destination.');
                        exit();
                    }

                    log.trace('Publish successful.');
                    queue = queue.slice(items.length);
                    mirroredMessages += items.length;

                    if (queue.length == 0) {
                        log.trace('Committing offset.');
                        consumer.commit(true, function (err, data) {
                            if (err) {
                                log.error(err, 'Failed to commit offset.');
                                return exit();
                            }

                            log.trace('Commit succesful.');

                            // resume the consumer if we have paused it and the process is not marked to be stopped
                            if (!stop && pauseOnSend) {
                                consumer.resume();
                                log.trace('Resuming consumer.');
                            }

                            // only keep going if we haven't received the stop signal
                            // OR if we have received it and there is still stuff in the queue.
                            if (!stop || queue.length > 0) {
                                setTimeout(publish, publishDelay);
                            }
                            else {
                                stopped = true;
                            }
                        });
                    }
                    else setTimeout(publish, publishDelay);
                });

        },

        monitorDelay = 1000,
        lastMirroredMessages = 0,
        monitor = function () {
            log.debug({queueLength: queue.length, mirroredMessages: mirroredMessages, mirroredMessagesPerSecond: mirroredMessages - lastMirroredMessages });
            lastMirroredMessages = mirroredMessages;
        },
        monitorInterval = setInterval(monitor, monitorDelay);

    consumer.on('message', function (m) {
        queue.push(m.value);
    });

    consumer.on('error', function (e) {
        log.error(e, 'Error receiving messages from source.');
        exit(1);
    });

    consumer.on('offsetOutOfRange', function (e) {
        log.warn(e, 'Error receiving messages from source.');
    });

    process.on('SIGINT', function () {
        log.info('Shutting down...');
        exit(1);
    });

    publishTimeout = setTimeout(publish, publishDelay);

};
