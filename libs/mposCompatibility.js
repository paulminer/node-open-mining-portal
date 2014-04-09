var mysql = require('mysql');
var cluster = require('cluster');
module.exports = function(logger, poolConfig){

    var mposConfig = poolConfig.shareProcessing.mpos;
    var coin = poolConfig.coin.name;

    var connection;

    var logIdentify = 'MySQL';
    var logComponent = coin;

    function connect(){
        connection = mysql.createConnection({
            host: mposConfig.host,
            port: mposConfig.port,
            user: mposConfig.user,
            password: mposConfig.password,
            database: mposConfig.database
        });
        connection.connect(function(err){
             if (err) {
                logger.error(logIdentify, logComponent, 'Could not connect to mysql database: ' + JSON.stringify(err));
                connect();
            }
            else{
                logger.debug(logIdentify, logComponent, 'Successful connection to MySQL database');
            }
        });
        connection.on('error', function(err){
            if(err.code === 'PROTOCOL_CONNECTION_LOST') {
                logger.warning(logIdentify, logComponent, 'Lost connection to MySQL database, attempting reconnection...');
                connect();
            }
            else{
                logger.error(logIdentify, logComponent, 'Database error: ' + JSON.stringify(err))
            }
        });
    }
    connect();

    this.handleAuth = function(workerName, password, authCallback){

        connection.query(
            'SELECT password FROM pool_worker WHERE username = LOWER(?)',
            [workerName],
            function(err, result){
                if (err){
                    logger.error(logIdentify, logComponent, 'Database error when authenticating worker: ' +
                        JSON.stringify(err));
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
            shareData.ip,
            shareData.worker,
            isValidShare ? 'Y' : 'N', 
            isValidBlock ? 'Y' : 'N',
            shareData.difficulty,
            typeof(shareData.error) === 'undefined' ? null : shareData.error,
            shareData.blockHash ? shareData.blockHash : (shareData.blockHashInvalid ? shareData.blockHashInvalid : '')
        ];
        connection.query(
            'INSERT INTO `shares` SET time = NOW(), rem_host = ?, username = ?, our_result = ?, upstream_result = ?, difficulty = ?, reason = ?, solution = ?',
            dbData,
            function(err, result) {
                if (err)
                    logger.error(logIdentify, logComponent, 'Insert error when adding share: ' + JSON.stringify(err));
                else
                    logger.debug(logIdentify, logComponent, 'Share inserted');
            }
        );
    };

    this.handleDifficultyUpdate = function(workerName, diff){

        connection.query(
            'UPDATE `pool_worker` SET `difficulty` = ' + diff + ' WHERE `username` = ' + connection.escape(workerName),
            function(err, result){
                if (err)
                    logger.error(logIdentify, logComponent, 'Error when updating worker diff: ' +
                        JSON.stringify(err));
                else if (result.affectedRows === 0){
                    connection.query('INSERT INTO `pool_worker` SET ?', {username: workerName, difficulty: diff});
                }
                else
                    console.log('Updated difficulty successfully', result);
            }
        );
    };


};
