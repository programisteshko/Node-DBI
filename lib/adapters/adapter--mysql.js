/**
 * Adapter for Felixge's "mysql" API.
 * @see https://github.com/felixge
 * @see https://github.com/felixge/node-mysql
 * 
 * @author Dr. Benton - www.dr-benton.com
 * @see https://github.com/DrBenton/Node-DBI
 */

// ------------------------------------- modules

var async = require('async')
  , util = require('util')
  , _ = require('underscore')
  , DBAdapterAbstract = require('../dbAdapterAbstract').DBAdapterAbstract
  , mysql = require('mysql');

//------------------------------------- constructor

/**
 * @param {DBWrapper} dbWrapper
 * @param {Object} connectionParams
 */
function Adapter( dbWrapper, connectionParams )
{
  
  this._dbClient = null;
  /**
   * @type boolean
   */
  this._debug = false; 
  /**
   * @type Integer
   */
  this._lastInsertId = null;
  
  // "Super" constructor is called
  DBAdapterAbstract.call( this, dbWrapper, connectionParams );
  
}

util.inherits(Adapter, DBAdapterAbstract);


//------------------------------------- static stuff

/**
 * @param {DBWrapper} dbWrapper
 * @param {Object} connectionParams
 * @api public
 * @static
 */
Adapter.createInstance = function( dbWrapper, connectionParams )
{
  return new Adapter( dbWrapper, connectionParams );
};

module.exports.createInstance = Adapter.createInstance;


//------------------------------------- public methods

/**
* @api public
*/
Adapter.prototype.connect = function()
{
  
  if( this._debug )
    console.log('Adapter.prototype.connect('+JSON.stringify(this._connectionParams)+')');
  
  _.defaults( this._connectionParams, {
    host:     'localhost',
    port:     3306,
    user:     null,
    password: null,
    database: null
  } );
  
  this._dbClient = new mysql.Client();
  this._dbClient.host = this._connectionParams.host;
  this._dbClient.port = this._connectionParams.port;
  this._dbClient.user = this._connectionParams.user;
  this._dbClient.password = this._connectionParams.password;
  this._dbClient.database = this._connectionParams.database;
  
  this._dbClient.connect( _.bind( this._onConnectionInitialization, this ) );
  
};


/**
 * @param {String}  sql
 * @param {Array|null}  bind
 * @param {Function|null}  callback
 * @api public
 */
Adapter.prototype.query = function( sql, bind, callback )
{
  
  // SQL values are safely escaped ; since this client API doesn't handle it, we do it manually 
  sql = this._format(sql, bind);
  
  if( this._debug )
    console.log('sql='+sql);
  
  // Some aliases for the async scope...
  var me = this;
  var _dbClient = this._dbClient;
  
  // Go! Go! Go!
  async.waterfall(
      
    [
      function( callback )
      {
        _dbClient.query( sql, callback );
      }
    ],
    
    function( err, res )
    {
      
      if( ! callback )
        return;

      if( err )
        callback( err );
      else
      {
        
        if( res.insertId && ! isNaN(res.insertId) && res.insertId>0 )
          me._lastInsertId = res.insertId;
        
        callback( null, res.affectedRows );   
        
      }
    }
  );
  
};


/**
 * @param {String}  sql
 * @param {Array|null}  bind
 * @param {Function|null}  callback
 * @api public
 */
Adapter.prototype.fetchAll = function( sql, bind, callback )
{
  
  // SQL values are safely escaped ; since this client API doesn't handle it, we do it manually
  sql = this._format(sql, bind);
  
  // Some aliases for the async scope...
  var query = _.bind( this.query, this );
  var _dbClient = this._dbClient;
  var _debug = this._debug;
  
  
  if( _debug )
    console.log('sql='+sql);
  
  // Go! Go! Go!  
  async.waterfall(
      
    [
     function( callback )
     {
       _dbClient.query( sql, callback );
     },
    
    ],
    
    function( err, rows, fields )
    {
      
      if( _debug )
      {
        console.log('rows='+JSON.stringify(rows));
        console.log('fields='+JSON.stringify(fields));
      }
      
      callback && callback( err, rows );
      
    }
    
  );
  
};

/**
 * @rturn {Integer|null}  the last inserted Id
 * @api public
 */
Adapter.prototype.getLastInsertId = function()
{
  return this._lastInsertId; 
};


/**
 * @param {String}  value
 * @rturn {String}  the escaped value
 * @api public
 */
Adapter.prototype.escape = function( value )
{
  if( value instanceof Date )
    return "'" + this._stringifyDate(value) + "'";
  else if( isNaN(value) )
    return this._dbClient.escape( value );
  else
    return value;
};

