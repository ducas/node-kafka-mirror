var consumer = require('./lib/mirror'),
    MongoClient = require('mongodb'),
    Promise = require('promise'),
    args = require('yargs').argv,
    bunyan = require('bunyan'),

    clientId = args.clientId || 'kafka-mongo-mirror',
    mongoUrl = args.to_mongo_connect,
    collection = args.to_collection,
    logLevel = args.logLevel || 'info',

    log = bunyan.createLogger({ name: clientId, level: logLevel }),

    startConsumer = function (db) {
        consumer({
            args: args,
            clientId: clientId,
            log: log,
            producer: {
                send: function (items, callback) {
                    var operation = db.collection(collection).initializeOrderedBulkOp();
                    items.forEach(function (i) {
                        operation.insert(JSON.parse(i))
                    });
                    operation.execute(callback);
                },
                close: db.close
            },
            pauseOnSend: true
        });
    };

if (!mongoUrl) {
    log.error('argument to_mongo_connect not specified');
    return 1;
}
if (!collection) {
    log.error('argument to_collection not specified');
    return 1;
}

MongoClient.connect(mongoUrl, function (err, db) {
    if (err) {
        log.error(err, 'Failed to connect to mongo.');
        process.exit(1);
    }
    log.info('Ensuring collection ' + collection + ' exists.');
    db.createCollection(collection, function (e) {
        if (e) {
            log.error(e, 'Error creating collection.');
            process.exit(1);
        }
        startConsumer(db);
    });
});


