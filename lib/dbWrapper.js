/**
 * @class DBWrapper
 * @author Dr. Benton - github.com/DrBenton
 * @see https://github.com/DrBenton/Node-DBI
 */

// ------------------------------------- modules

var async = require('async')
  , _ = require('lodash')
  , DBAdapterAbstract = require('./dbAdapterAbstract').DBAdapterAbstract
  , DBExpr = require('./dbExpr').DBExpr
  , DBSelect = require('./dbSelect').DBSelect;

// ------------------------------------- constructor

/**
 * @param {String} dbAdapterName
 * @param {Array} connectionParams
 */
function DBWrapper( dbAdapterName, connectionParams ) {

    this.dbAdapterName = dbAdapterName
    this.connectionParams = connectionParams
  
  if( 2 > arguments.length )
    throw new Error('too few arguments given');

  if( -1 == DBWrapper._availableAdapters.indexOf( dbAdapterName ) )
    throw new Error('Unknown adapter "'+dbAdapterName+'" ! (should be one of '+DBWrapper._availableAdapters.join('|')+')');
}

module.exports.DBWrapper = DBWrapper;


// ------------------------------------- static stuff

/**
 * @api private
 * @static
 */
DBWrapper._availableAdapters = [
  'mysql-libmysqlclient', //@see https://github.com/Sannis/node-mysql-libmysqlclient
  'mysql',                //@see https://github.com/felixge/node-mysql
  'sqlite3',              //@see https://github.com/developmentseed/node-sqlite3
  'pg'                    //@see https://github.com/brianc/node-postgres
];

// ------------------------------------- public methods

/**
 * 
 * @param {Function} callback
 */
DBWrapper.prototype.connect = function( callback )
{
    this._adapter = require('./adapters/adapter--' + this.dbAdapterName ).createInstance( this, this.connectionParams );
  
    if (! (this._adapter instanceof DBAdapterAbstract) )
        return callback('Adapters must be instances of "'+DBAdapterAbstract+'"');
  
    const listener = (err, adapter) => {
        this._adapter.removeListener('connection', listener)
        if (err) {
            this._adapter = null
            return callback(err)
        }
        // adapter should be available just after successfull connect (other async chains could null this._adapter)
        this._adapter = adapter
        callback(null, adapter)
    }

    this._adapter.addListener('connection', listener)
    this._adapter.connect()
}

/**
 * 
 * @param {Function} callback
 */
DBWrapper.prototype.close = function( callback ) {
    this._adapter.close(callback)
    this._adapter = null
}

function unpackSelectArgs(args) {
    callback = args.pop()
    if (args.length == 0) {
        return [[], callback]
    } else if (typeof args[0] == 'object') {
        if (args.length != 1)
            throw Error("Wrong arguments to db wrapper")
        else
            return [args[0], callback]
    } else
        return [args, callback]
}

/**
 * Prepares and executes an SQL statement with bound data.
 * 
 * @param {String|DBSelect}  sql               The SQL statement with placeholders.
 *                                              May be a string or DBSelect.
 * @param {Array|null}  bind                   An array of data to bind to the placeholders.
 * @param {Function|null}  callback            The callback function receives the error, if any, and the number of affected rows
 * @api public
 */
DBWrapper.prototype._query = function(sql, ...args) {
    let [bind, callback] = unpackSelectArgs(args)
    if ( sql instanceof DBSelect )
        sql = sql.assemble()
    this._adapter.query(sql, bind, callback)
}

/**
 * Fetches all SQL result rows as a Array.
 * 
 * @param {String|DBSelect}  sql              An SQL SELECT statement.
 * @param {Array|null}  bind                  Data to bind into SELECT placeholders.
 * @param {Function|null}  callback           The callback function receives the error, if any, and all the result rows
 * @api public
 */
DBWrapper.prototype._fetchAll = function(sql, ...args) {
    let [bind, callback] = unpackSelectArgs(args)
    if (sql instanceof DBSelect)
        sql = sql.assemble()
    this._adapter.fetchAll(sql, bind, callback)
}

DBWrapper.prototype.connectIfNotConnected = function(callback) {
    if (this._adapter)
        return callback(null, this._adapter)
    this.connect(callback)
}

DBWrapper.prototype.fetchAll = function(...args) {
    const callback = args.pop()

    this.connectIfNotConnected((err) => {
        if (err)
            return callback(err)
        this._fetchAll(...args, (err, result) => {
            if (err) {
                this.connect((err) => {
                    if (err)
                        return callback(err)
                    this._fetchAll(...args, callback)
                })
            } else
                callback(null, result)
        })
    })
}


/**
 * Fetches the first row of the SQL result.
 * 
 * @param {String|DBSelect}  sql              An SQL SELECT statement.
 * @param {Array|null}  bind                  Data to bind into SELECT placeholders.
 * @param {Function|null}  callback           The callback function receives the error, if any, and the first result row
 * @api public
 */
DBWrapper.prototype.fetchRow = function(sql, ...args) {
    let [bind, callback] = unpackSelectArgs(args)
  
    async.waterfall([
        callback => this.fetchAll( sql, bind, callback )
    ], function (err, res) {
        if( err )
          callback( err );
        else if( res && res.length>0 )
          callback( null, res[0] );//only the first row is returned to the callback
        else
          callback( null, null );//no result
    })
}


/**
 * Fetches the first column of all SQL result rows as an Array.
 * 
 * @param {String|DBSelect}  sql              An SQL SELECT statement.
 * @param {Array|null}  bind                  Data to bind into SELECT placeholders.
 * @param {Function|null}  callback           The callback function receives the error, if any, and an array populated with the first column value of every result rows
 * @api public
 */
DBWrapper.prototype.fetchCol = function(sql, ...args) {
    let [bind, callback] = unpackSelectArgs(args)
    
    async.waterfall([
        callback => this.fetchAll(sql, bind, callback)
    ], function (err, res) {
        if (err)
            return callback(err)
        var returnedArray = []
        if(!!res) {
            var firstFieldName = _.keys( res[0] )[0]
            for ( var i=0, j=res.length; i<j; i++ )
                returnedArray.push( res[i][firstFieldName] ); //only the first col of each row is returned to the callback
        }
        callback( null, returnedArray )
    })
}


/**
 * Fetches the first column of the first row of the SQL result.
 * 
 * @param {String|DBSelect}  sql              An SQL SELECT statement.
 * @param {Array|null}  bind                  Data to bind into SELECT placeholders.
 * @param {Function|null}  callback           The callback function receives the error, if any, and the value of the first column of the first result row
 * @api public
 */
DBWrapper.prototype.fetchOne = function(sql, ...args) {
    let [bind, callback] = unpackSelectArgs(args)
  
    async.waterfall([
        callback => this.fetchRow(sql, bind, callback),
    ], (err, res) => {
        if (err)
            callback(err)
        else if( ! res )
            callback() //no result
        else {
            var firstFieldName = _.keys( res )[0]
            callback( null, res[firstFieldName] ) //only the first col of the first row is returned to the callback
        }
    })
}


/**
 * Inserts a table row with specified data.
 * 
 * @param {tableName}  tableName          The table to insert data into.
 * @param {Object}  data                  Column-value pairs.
 * @param {Function|null}  callback       The callback function receives the error, if any, and the number of affected rows
 * @api public
 */
DBWrapper.prototype._insert = function( tableName, data, callback ) {
  // Some params check
  if( ! data || _.isEmpty(data) )
  {
    callback( new Error('DBWrapper.insert() called without data !') );
    return;
  }
  
  // SQL initialization  
  var sql = 'INSERT INTO ' + this._adapter.escapeTable(tableName);

  // Fields values management
  var sqlFieldsStrArray = [];
  var sqlValuesArray = [];
  var valuesPlaceholders = [];
  for( var fieldName in data )
  {
    sqlFieldsStrArray.push( this._adapter.escapeField(fieldName) );
    sqlValuesArray.push( data[fieldName] );
    valuesPlaceholders.push( '?' );
  }
  
  sql += '(' + sqlFieldsStrArray.join(', ') + ') VALUES(' + valuesPlaceholders.join(', ') + ')';
  // Go! Go! Go!
  this._query(sql, sqlValuesArray, callback)
}


/**
 * Updates table rows with specified data based on a WHERE clause.
 * 
 * @param {tableName}  tableName          The table to update.
 * @param {Object}  data                  Column-value pairs.
 * @param {String|Array}  where           UPDATE WHERE clause(s).
 * @param {Function|null}  callback       The callback function receives the error, if any, and the number of affected rows
 * @api public
 */
DBWrapper.prototype._update = function( tableName, data, where, callback ) {
  // Some params check
  if( ! data || _.isEmpty(data) )
  {
    callback( new Error('DBWrapper.update() called without data !') );
    return;
  }
  if( ! where || (typeof(where)=='string' && where.length==0) || (typeof(where)=='object' && _.isEmpty(where)) )
  {
    callback( new Error('DBWrapper.update() called without where !') );
    return;
  }
  
  // SQL initialization
  var sql = 'UPDATE ' + this._adapter.escapeTable(tableName) + ' SET ';
  
  // Fields values management
  var sqlFieldsStrArray = [];
  var sqlValuesArray = [];
  for( var fieldName in data )
  {
    sqlFieldsStrArray.push( this._adapter.escapeField(fieldName) + '=?' );
    sqlValuesArray.push( data[fieldName] );
  }
  
  sql += sqlFieldsStrArray.join(', ');
  
  
  // WHERE clause construction 
  sql += ' WHERE ' + this._whereExpr( where );
      
  // Go! Go! Go!
  this._query(sql, sqlValuesArray, callback)
}


/**
 * Deletes table rows based on a WHERE clause.<br/>
 * This function name should have been named "delete", but this is a reserved word of Javascript... :-(
 * 
 * @param {tableName}  tableName          The table to update.
 * @param {String|Array}  where           DELETE WHERE clause(s).
 * @param {Function|null}  callback       The callback function receives the error, if any, and the number of affected rows
 * @api public
 */
DBWrapper.prototype._remove = function( tableName, where, callback ) {
  if( 3 > arguments.length )
    throw new Error('too few arguments given');
  
  // Some params check
  if( ! where || (typeof(where)=='string' && where.length==0) || (typeof(where)=='object' && _.isEmpty(where)) ) {
    callback( new Error('DBWrapper.update() called without where !') );
    return;
  }
  
  // SQL initialization
  var sql = 'DELETE FROM ' + this._adapter.escapeTable(tableName);
    
  // WHERE clause construction 
  sql += ' WHERE ' + this._whereExpr( where );
      
  // Go! Go! Go!
  this._query(sql, [], callback);   
}


/**
 * Safely quotes a value for an SQL statement.
 *
 * If an Array is passed as the value, the Array values are quoted
 * and then returned as a comma-separated string.
 * 
 * If "value" is a DBExpr, its method "toString()" is triggered and no escaping is done.
 * 
 * @param {String|Array|DBExpr}  value  The value to quote.
 * @returns {String}                    The escaped value
 * @api public
 */
DBWrapper.prototype.escape = function( value )
{
  if( value instanceof DBExpr )
    return value.toString();
  else if( value instanceof Array )
  {
    var returnedValues = [];
    for( var i=0, j=value.length; i<j; i++ )
      returnedValues.push( this._adapter.escape( value[i] ) );
    return returnedValues.join(', ');
  }
  else
    return this._adapter.escape( value );
}


/**
 * @returns {Integer|null}  the last inserted Id
 * @api public
 */
DBWrapper.prototype.getLastInsertId = function() {
    return this._adapter.getLastInsertId()
}


/**
 * @returns {Boolean}  
 * @api public
 */
DBWrapper.prototype.isConnected = function() {
    return this._adapter != null
}


/**
 * @returns {DBSelect}  Returns a new DBSelect instance
 * @api public
 */
DBWrapper.prototype.getSelect = function() {
    return this._adapter.getSelect()
}

// ------------------------------------- private methods

/**
 * Convert an Array, String, or DBSelect object
 * into a string to put in a WHERE clause.<br/>
 * This code is a straight conversion of the PHP Zend Framework's
 * Zend_Db_Adapter_Abstract "_whereExpr()" method.
 *  
 * @see http://framework.zend.com/manual/fr/zend.db.adapter.html
 * 
 * @param {String|Array|DBSelect} $where
 * @return {String}
 * @api protected
 */
DBWrapper.prototype._whereExpr = function(where) {
  if ( ! where)
    return where

  if ( ! _.isArray(where) )
    where = [ where ];
  
  var result = [];
  for ( var i=0, j=where.length; i<j; i++ )
  {
    var term = where[i];
    if( typeof(term)=='string' )
    {
      // ...nothing to do
    }
    else if ( term instanceof DBSelect )
    {
      term = term.assemble();
    }
    else if( _.isArray( term ) )
    {
      // cond is the condition with placeholder,
      // and term is quoted into the condition
      term = term[0].replace( /\?/g, this.escape(term[1]) );
    }
    result.push( '(' + term + ')' );
  }
  
  return result.join(' AND ')
}
