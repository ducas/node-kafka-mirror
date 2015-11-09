var kafka = require('kafka-node'),
    Promise = require('promise'),
    args = require('yargs').argv,

    clientId = args.clientId || 'kafka-mirror';

var client = new kafka.Client(args.zk_connect, clientId);

var producer = new kafka.HighLevelProducer(client, { requireAcks: 1, ackTimeoutMs: 100 });

var exit = function () {
    producer.close(function () {
        process.exit();
    });
}

producer.on('ready', function () {
    producer.createTopics([args.topic], false, function () {});

    var i = 0;

    setInterval(function () {
        if (i < 1000) {

            producer.send(
                [{
                    topic: args.topic,
                    messages: i,
                    attributes: 2
                }], function (e) {
                    if (e) {
                        console.error("Error sending message to destination: " + e);
                        return exit();
                    }
                    process.stdout.write('.');
                });

            i++;
        }
        else {
            exit();
        }
    }, 100);
});