/**
 * mysql socket
 * USER: chenlingguang
 * TIME: 15/2/9 下午5:52
 */

var mysql = require('mysql');
var extend = require('../Common/common').extend;
var getDefer = require('../Common/common').getDefer;

function MysqlSocket (config) {
  'use strict';
  this.config = config;
  this.init();
}

exports = module.exports = MysqlSocket;

MysqlSocket.prototype.init = function () {
  'use strict';
  var self = this;
  //创建连接
  self.pool = mysql.createPool(self.config);
};

/**
 * 执行sql
 * @param sql
 * @returns {*|type[]}
 */
MysqlSocket.prototype.query = function (sql) {
  'use strict';
  var self = this;
  if (self.config.logSql) {
    console.log('sql: ' + sql);
  }
  var deferred = getDefer();
  self.pool.query(sql, function (err, rows) {
    if (err) {
      deferred.reject(err);
    } else {
      deferred.resolve(rows);
    }
  });
  return deferred.promise;
};


MysqlSocket.prototype.transaction = function(){
  'use strict';
  var self = this;
  var deferred = getDefer();
  self.pool.getConnection((err, connection) => {
    if (err) {
      deferred.reject(err);
    }else{
      connection.beginTransaction((err)=>{
        if (err) {
          deferred.reject(err);
        }else{
          deferred.resolve(connection);
        }
      })
    }
  })
  return deferred.promise;
}

MysqlSocket.prototype.t_query = function(t,sql){
  'use strict';
  var deferred = getDefer();
  t.query(sql,  (err, rows) =>{
    if (err) {
      deferred.reject(err);
    } else {
      deferred.resolve(rows);
    }
  });
  return deferred.promise;
}


MysqlSocket.prototype.t_commit = function(t){
  'use strict';
  var deferred = getDefer();
  t.commit((err)=>{
    if (err) {
      deferred.reject(err);
    } else {
      deferred.resolve(t);
    }
    t.release()
  })
  return deferred.promise;
}

MysqlSocket.prototype.t_rollback = function(t){
  'use strict';
  var deferred = getDefer();
  t.rollback((err)=>{
    if (err) {
      deferred.reject(err);
    } else {
      deferred.resolve(t);
    }
    t.release()
  })
  return deferred.promise;
}

MysqlSocket.prototype.t_release = function(t){
  'use strict';
  t.release()
}







/**
 * 关闭连接
 */
MysqlSocket.prototype.close = function () {
  'use strict';
  this.pool.end()
};

