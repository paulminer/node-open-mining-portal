var mysql = require('mysql');
var cluster = require('cluster');
var redis = require('redis');

module.exports = function(logger, poolConfig, redisOptions) {

    var mposConfig = poolConfig.shareProcessing.mpos;
    var coin = poolConfig.coin.name;

    var connection;

    var logIdentify = 'MySQL';
    var logComponent = coin;
    
    function zeroPad(n, len) {
        var s = '' + n;
        
        while (s.length < len) {
            s = '0' + s;
        }
        
        return s;
    }
    
    function formatMySQLDateString(d) {
        // 'YYYY-MM-DD HH:MM:SS'
        return zeroPad(d.getFullYear(),  4) + '-' +
               zeroPad(d.getMonth() + 1, 2) + '-' +
               zeroPad(d.getDate(),      2) + ' ' +
               zeroPad(d.getHours(),     2) + ':' +
               zeroPad(d.getMinutes(),   2) + ':' +
               zeroPad(d.getSeconds(),   2);
    }
    
    function connect() {
        connection = mysql.createConnection({
            host: mposConfig.host,
            port: mposConfig.port,
            user: mposConfig.user,
            password: mposConfig.password,
            database: mposConfig.database
        });
        
        connection.on('error', function(err) {
            if (err.code === 'PROTOCOL_CONNECTION_LOST') {
                logger.warning(logIdentify, logComponent, 'Lost connection to MySQL database, exiting...');
                process.exit(1);
            }
            else {
                logger.error(logIdentify, logComponent, 'Database error: ' + JSON.stringify(err))
            }
        });
        
        connection.connect(function(err) {
            if (err) {
                logger.error(logIdentify, logComponent, 'Could not connect to mysql database: ' + JSON.stringify(err))
            }
            else {
                logger.debug(logIdentify, logComponent, 'Successful connection to MySQL database');
            }
        });
    }
    
    connect();
    
    logger.error(logIdentify, logComponent, 'redisOptions.redisPort=' + redisOptions.redisPort + ' redisOptions.redisHost=' + redisOptions.redisHost);
    var redisConnection = redis.createClient(redisOptions.redisPort, redisOptions.redisHost);

    this.handleAuth = function(workerName, password, authCallback){

        connection.query(
            'SELECT password FROM pool_worker WHERE username = LOWER(?)',
            [workerName],
            function(err, result){
                if (err) {
                    logger.error(logIdentify, logComponent, 'Database error when authenticating worker: ' + JSON.stringify(err));
                    authCallback(false);
                }
                else if (!result[0])
                    authCallback(false);
                else if (mposConfig.stratumAuth === 'worker')
                    authCallback(true);
                else if (result[0].password === password)
                    authCallback(true)
                else
                    authCallback(false);
            }
        );

    };

    this.handleShare = function(isValidShare, isValidBlock, shareData){

        var dbData = [
            formatMySQLDateString(new Date()),
            shareData.ip,
            shareData.worker,
            isValidShare ? 'Y' : 'N', 
            isValidBlock ? 'Y' : 'N',
            shareData.difficulty,
            typeof(shareData.error) === 'undefined' ? null : shareData.error,
            shareData.blockHash ? shareData.blockHash : (shareData.blockHashInvalid ? shareData.blockHashInvalid : '')
        ];
        
        redisConnection.publish('addshare-' + coin, JSON.stringify(dbData));
        
        return;
    };

    this.handleDifficultyUpdate = function(workerName, diff){

        connection.query(
            'UPDATE `pool_worker` SET `difficulty` = ' + diff + ' WHERE `username` = ' + connection.escape(workerName),
            function(err, result){
                if (err) {
                    logger.error(logIdentify, logComponent, 'Error when updating worker diff: ' + JSON.stringify(err));
                }
                else if (result.affectedRows === 0){
                    connection.query('INSERT INTO `pool_worker` SET ?', {username: workerName, difficulty: diff});
                }
                else {
                    console.log('Updated difficulty successfully', result);
                }
            }
        );
    };


};
