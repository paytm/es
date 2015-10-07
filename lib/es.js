/*jshint multistr: true ,node: true */
"use strict";

var 
  UTIL           = require('util'),
  EVENTEMITTER   = require('events').EventEmitter,
  _              = require('lodash'),
  REQUEST        = require('request'),
  L              = require('lgr'),
  Q              = require('q');



/*
 * The idea is to bind a query to one of the esCursor objects
 * and give it a pageSize parameter.
 */

UTIL.inherits(es, EVENTEMITTER);

function es (opts) {
  /*
  * The opts must contain requestOpts  
  */
  
  var self = this;

  EVENTEMITTER.call(self);
 
  if(!opts['requestOpts']){
    throw new Error("Provide requestOpts to initialize the cursor successfully");
  }
  
  self.host      = _.get(opts,"requestOpts.host",''); 
  self.pfx       = _.get(opts,"requestOpts.pfx",''); 
  self.pass      = _.get(opts,"requestOpts.pass",''); 
}

es.prototype._getThreadPoolStatus = function () {
  var self = this;

  var deferred = Q.defer();

  var threadPoolURI = self.host + '/' + "_cat/thread_pool?h=host,bulk.queue";

  REQUEST(threadPoolURI, function (error , response) {
    if (error) {
      deferred.reject(error);
    }

    if (response.statusCode !== 200) {
      deferred.reject(response.body);
    }

    var responseJson = [];

    // Parsing the response given by the server
    response.body = response.body.split('\n');

    response.body.forEach(function (node) {
      var host = node.split(' ')[0];
      var queueCount = node.split(' ')[1];

      if (host !== undefined && queueCount !== undefined) {
        responseJson.push({
          host : host,
          queueCount : queueCount
        });
      }
    });

    deferred.resolve(responseJson);
    
  });
  
  return deferred.promise;
};

es.prototype.paginate = function (pageSize) {
  var self = this;

  //setting params for query
  self.currentPage  = self.currentPage || 0;
  self.query.from   = self.from || 0;
  self.query.size   = self.pageSize;

  var requestOpts = {
    uri        : self.uri,
    method     : "GET",
    json       : self.query,
    pfx        : require('fs').readFileSync(self.pfx),
    passphrase : self.pass
  };

  REQUEST(requestOpts, function (error, response) {
    if (error) {
      self.emit('error', error.message);
    } else if (response.statusCode !== 200) {
      self.emit('error', response.body.toString());
    } else {

      self.from = (++self.currentPage * self.pageSize);
      if (response.body.hits.hits.length) {
        /*
        * Returning the following params -
        * - raw body
        * - no of hits got i.e. length
        * - hits ie. array with _source and everything
        * - aggs 
        */
        self.emit('data', response.body,response.body.hits.hits.length,response.body.hits.hits,response.body.aggregations);
      } else {
        self.emit('end' , response.body);
      }
    }    
  });  
};

es.prototype.setQuery = function (uri, query) {
  var self = this;

  if (!query || !uri) {
    throw new Error("Please provide complete params to paginate upon");
  }

  self.uri  = self.host + '/'  + uri;
  self.query = query;
};

es.prototype.setPageSize = function (pageSize) {
  this.pageSize = pageSize;
};

es.prototype.bulkIndex = function (index, type, data) {

  var self = this;

  var bulkIndexString = '';

  var highQueueCount = false;
  var queueCountThreshold = 10;

  function _startIndexing () {
    if (!UTIL.isArray(data)) data = [data];

    data.forEach(function (row) {

      // Expecting row to have an id at the first level
      var data_header = {
	index : {
          _index : index,
          _type : type,
          _id : row.id
	}
      };

      bulkIndexString += JSON.stringify(data_header) + '\n' + JSON.stringify(row) + '\n';
    });

    var bulkIndexOptions = {
      method : "POST",
      url : self.host + "/_bulk",
      body : bulkIndexString
    };

    REQUEST(bulkIndexOptions , function (error , response) {

      if (error) {
	self.emit('error', error.message);
      }

      if (response.statusCode !== 200) {
	self.emit('error', response.body);
      }

      self.emit('done');

    });
  }

  self._getThreadPoolStatus()
    .then(function (threadPoolStatus) {
      // This would be an array of nodes and queue count
      threadPoolStatus.forEach(function (node) {
	if (node.queueCount > queueCountThreshold) {
          highQueueCount = true;
	}
      });

      if (highQueueCount) {
	self.highQueueCount = true;
	self.emit('error', new Error ("Cannot accept requests for bulk indexing because of high queue count").message);
      } else {
	self.highQueueCount = false;
	_startIndexing();
      }
    })
    .fail(function (error) {
      self.emit('error', error.message);
    });
};

module.exports = es;
