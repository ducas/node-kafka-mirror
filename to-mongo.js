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
                    db.collection(args.to_collection).insertMany(
                        items.map(JSON.parse),
                        callback
                    );
                },
                close: db.close
            },
            pauseOnSend: true
        });
    };

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


