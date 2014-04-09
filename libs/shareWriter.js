var mysql = require('mysql');
var redis = require('redis');

module.exports = function(logger, poolConfig, redisOptions) {

    var mposConfig = poolConfig.shareProcessing.mpos;
    var coin = poolConfig.coin.name;
    var logIdentify = 'MySQL';
    var logComponent = coin;
    
    var MAX_SHARE_QUEUE_SIZE = 50;
    var MAX_BULK_INSERT_SIZE = 50;
    var MAX_SHARE_QUEUE_AGE_MILLIS = 2000;
    var SHARE_QUEUE_CHECK_INTERVAL_MILLIS = 500;
    
    var connection;

    function connectSQL() {
        connection = mysql.createConnection({
            host: mposConfig.host,
            port: mposConfig.port,
            user: mposConfig.user,
            password: mposConfig.password,
            database: mposConfig.database
        });
        
        connection.on('error', function(err) {
            if (err.code === 'PROTOCOL_CONNECTION_LOST') {
                logger.warning(logIdentify, logComponent, 'Lost connection to MySQL database, attempting reconnection...');
                connectSQL();
            }
            else {
                logger.error(logIdentify, logComponent, 'Database error: ' + JSON.stringify(err))
            }
        });
        
        connection.connect(function(err) {
            if (err) {
                logger.error(logIdentify, logComponent, 'ShareWriter could not connect to mysql database: ' + JSON.stringify(err))
            }
            else {
                logger.debug(logIdentify, logComponent, 'ShareWriter successful connection to MySQL database');
            }
        });
    }
    
    var shareQueue = [];
    var shareQueueOldestTime = -1;
    
    function enqueueShare(data) {
        shareQueue.push(data);
        processShareQueueIfReady();
        return;
    };
    
    function processShareQueueIfReady() {
        if (shareQueue.length > 0) {
            
            if ((shareQueue.length >= MAX_SHARE_QUEUE_SIZE) || ((Date.now() - shareQueueOldestTime) >= MAX_SHARE_QUEUE_AGE_MILLIS)) {
                shareQueueOldestTime = Date.now();
                while (shareQueue.length > 0) {
                    var curShareQueue = [];
                    while ((shareQueue.length > 0) && (curShareQueue.length < MAX_BULK_INSERT_SIZE)) {
                        curShareQueue.push(shareQueue.shift());
                    }
                    
                    (function(curShareQueue) {
                        connection.query(
                            'INSERT INTO `shares` (time, rem_host, username, our_result, upstream_result, difficulty, reason, solution) VALUES ?',
                            [curShareQueue],
                            function(err, result) {
                                if (err) {
                                    logger.error(logIdentify, logComponent, 'Insert error when adding share(s): ' + JSON.stringify(err) + ' remaining=' + shareQueue.length);
                                }
                                else {
                                    logger.debug(logIdentify, logComponent, 'Shares inserted: ' + curShareQueue.length + ' remaining=' + shareQueue.length);
                                }
                                return;
                            }
                        );
                    })(curShareQueue);
                }
            }
        }
        
        return;
    };
    
    function connectRedis() {
        var redisConnection = redis.createClient(redisOptions.redisPort, redisOptions.redisHost);
        redisConnection.on("message", function (channel, message) {
            var dbData = JSON.parse(message);
            enqueueShare(dbData);
            return;
        });
        
        redisConnection.subscribe('addshare-' + coin);
        logger.debug(logIdentify, logComponent, "ShareWriter for coin " + coin + " connected to redis!");
        return;
    }
    
    connectSQL();
    
    connectRedis();
    
    setInterval(processShareQueueIfReady, SHARE_QUEUE_CHECK_INTERVAL_MILLIS);
    
};
