var kafka = require('kafka-node'),
    Promise = require('promise'),
    args = require('yargs').argv,
    util = require('util'),

    clientId = args.clientId || 'kafka-dump',

    client = new kafka.Client(args.zk_connect, clientId);

var offset = new kafka.Offset(client);

var earliest = new Promise(function (resolve, reject) {
    offset.fetch([{
            topic: args.topic,
            partition: 0,
            time: -2,
            maxNum: 1
        }],
        function (e, d) {
            if (e) {
                reject(e);
            }
            resolve(d);
        });
});
var latest = new Promise(function (resolve, reject) {
    offset.fetch([{
            topic: args.topic,
            partition: 0,
            time: -1,
            maxNum: 1
        }],
        function (e, d) {
            if (e) {
                reject(e);
            }
            resolve(d);
        });
});

return Promise.all([earliest, latest])
    .then(function (d) {
        var start = d[0][args.topic][0][0],
            end = d[1][args.topic][0][0];

        var consumer = new kafka.Consumer(
            client, [{
                topic: args.topic,
                partition: 0,
                offset: start
            }], {
            //consumer group id
            groupId: clientId,
            // Auto commit config 
            autoCommit: false,
            // The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available at the time the request is issued, default 100ms 
            fetchMaxWaitMs: 100,
            // This is the minimum number of bytes of messages that must be available to give a response, default 1 byte 
            fetchMinBytes: 1,
            // The maximum bytes to include in the message set for this partition. This helps bound the size of the response. 
            fetchMaxBytes: 1024 * 10,
            // If set true, consumer will fetch message from the given offset in the payloads 
            fromOffset: true,
            // If set to 'buffer', values will be returned as raw buffer objects. 
            encoding: 'utf8'
        }),
        exit = function () {
            consumer.close(function () { process.exit(); })
        };

        consumer.on('message', function (m) {
            console.log(util.inspect(m));
            if (m.offset == end - 1) exit();
        });

        consumer.on('error', function (e) {
            console.error('Error receiving messages from source: ' + e);
            exit();
        });

        consumer.on('offsetOutOfRange', function (e) {
            console.error('Error receiving messages from source: ' + e);
            exit();
        });

        process.on('SIGINT', function () {
            console.info('Shutting down...');
            exit();
        });
    });