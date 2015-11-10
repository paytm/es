/*jshint multistr: true ,node: true */
"use strict";

var
  UTIL          = require('util'),
  EVENTEMITTER  = require('events').EventEmitter,
  _             = require('lodash'),
  REQUEST       = require('request'),
  L             = require('lgr'),
  Q             = require('q');

/*
 * The idea is to bind a query to one of the esCursor objects
 * and give it a pageSize parameter.
 */

UTIL.inherits(es, EVENTEMITTER);

function es(opts) {
  /*
   * The opts must contain requestOpts  
   */

  var self = this;

  EVENTEMITTER.call(self);

  if (!opts['requestOpts']) {
    throw new Error("Provide requestOpts to initialize the cursor successfully");
  }

  self.host = _.get(opts, "requestOpts.host", '');
  self.pfx  = _.get(opts, "requestOpts.pfx", false);
  self.pass = _.get(opts, "requestOpts.pass", false);
};

es.prototype._getRequestOptsStencil = function (uri, method, query) {
  var requestOpts = {
    uri         : uri,
    method      : method,
    json        : query
  };

  if( this.pfx && this.pass ){    
    requestOpts.pfx          = require('fs').readFileSync(this.pfx);
    requestOpts.passphrase   = this.pass;
  }

  return requestOpts;
};

es.prototype._getThreadPoolStatus = function() {
  var 
    self              = this,
    deferred          = Q.defer(),
    threadPoolOpts    = self._getRequestOptsStencil(self.host + '/' + "_cat/thread_pool?h=host,bulk.queue", "GET"),    
    responseJson      = [];

    if(self.pfx&&self.pass){
      threadPoolOpts.pfx         = require('fs').readFileSync(self.pfx);
      threadPoolOpts.passphrase  = self.pass;
    }

  REQUEST(threadPoolOpts, function(error, response) {
    if (error) {
      return deferred.reject(error);
    }

    if (response.statusCode !== 200) {
      return deferred.reject(response.body);
    }

    // Parsing the response given by the server
    response.body = response.body.split('\n');

    response.body.forEach(function(node) {
      var host = node.split(' ')[0];
      var queueCount = node.split(' ')[1];

      if (host !== undefined && queueCount !== undefined) {
        responseJson.push({
          host: host,
          queueCount: queueCount
        });
      }
    });

    deferred.resolve(responseJson);

  });

  return deferred.promise;
};

es.prototype.paginate = function(pageSize) {
  var 
    self        = this,
    requestOpts = self._getRequestOptsStencil(self.uri, "GET", self.query);

  //setting params for query
  self.currentPage = self.currentPage || 0;
  self.query.from  = self.from || 0;
  self.query.size  = self.pageSize;

  REQUEST(requestOpts, function(error, response) {
    if (error) {
      self.emit('error', error.message);
    } else if (response.statusCode !== 200) {
      self.emit('error', response.body);
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
        self.emit('data', response.body, response.body.hits.hits.length, response.body.hits.hits, response.body.aggregations);
      } else {
        self.emit('end', response.body);
      }
    }
  });
};

es.prototype.setQuery = function(uri, query) {
  var self = this;

  if (!query || !uri) {
    throw new Error("Please provide complete params to paginate upon");
  }

  self.uri   = self.host + '/' + uri;
  self.query = query;
};

es.prototype.setPageSize = function(pageSize) {
  this.pageSize = pageSize;
};

es.prototype.bulkIndex = function(index, type, data, opts,cb) {

  var 
    self                = this,
    bulkIndexString     = '',
    highQueueCount      = false,
    queueCountThreshold = 10;

  /*
   Check that opts.operation is one of these values
   index,
   delete,
   create,
   update
   */

  function _startIndexing() {
    if(typeof data === 'string'){
      bulkIndexString = data;
    } else{
      if (!UTIL.isArray(data)) data = [data];

      data.forEach(function(row) {
        var 
          data_header = {},
          container = {};

        // Expecting row to have an id at the first level
        data_header[opts.operation] = {
          _index  : index,
          _type   : type,
          _id     : row.id
        };

        if (opts.operation === 'delete') {
          bulkIndexString += JSON.stringify(data_header) + '\n';
        } else if (opts.operation === 'update') {
          container = {
            doc: row.data
          };

          if (opts.upsert) {
            container.doc_as_upsert = true;
          }

          bulkIndexString += JSON.stringify(data_header) + '\n' + JSON.stringify(container) + '\n';

        } else {
          bulkIndexString += JSON.stringify(data_header) + '\n' + JSON.stringify(row) + '\n';
        }
      });
    }

    var bulkIndexOptions  = self._getRequestOptsStencil(self.host + "/_bulk", "POST");
    bulkIndexOptions.body = bulkIndexString;

    REQUEST(bulkIndexOptions, function(error, response) {

      if (error) {
        return cb ? cb(error): self.emit('error', error);
      }

      if (response.statusCode !== 200) {
        return cb ? cb(response.body): self.emit('error', response.body);
      }
      
      return cb ? cb(null, response.body): self.emit('done', response.body);

    });
  }

  self._getThreadPoolStatus()
    .then(function(threadPoolStatus) {

      // This would be an array of nodes and queue count
      threadPoolStatus.forEach(function(node) {
        if (node.queueCount > queueCountThreshold) {
          highQueueCount = true;
        }
      });

      if (highQueueCount) {
        self.highQueueCount = true;
        var error = new Error("Cannot accept requests for bulk indexing because of high queue count");
        return cb ? cb(error): self.emit('error', error);
      } else {
        self.highQueueCount = false;
        _startIndexing();
      }

    })
    .fail(function(error) {
      return cb ? cb(error): self.emit('error', error);
    });
};

es.prototype.instantiateScroll = function(pageSize, cb){
  var 
    self        = this,
    scroll_id,
    requestOpts = self._getRequestOptsStencil(self.uri, "GET", self.query);

  self.query.size = self.pageSize;
  self.uri        = self.uri + '?search_type=scan&scroll=1m';

  REQUEST(requestOpts, function(error, response) {
    if (error) {
      return cb ? cb(error): self.emit('error', error);
    } else if (response.statusCode !== 200) {
      return cb ? cb(response.body): self.emit('error', response.body);
    } else {
      self.scroll_id = response.body._scroll_id;
      return cb ? cb(null): self.emit('start');
    }
  });

};

es.prototype.scroll = function(cb) {

  var
    self        = this,
    scroll_id   = self.scroll_id,
    requestOpts = self._getRequestOptsStencil(self.host + '/_search/scroll?scroll=1m&scroll_id=' + scroll_id, "GET");

  REQUEST(requestOpts,function(error,response){
    if(error){
      return cb ? cb(error): self.emit('error', error); 
    } else if(response.statusCode !== 200){
      return cb ? cb(response.body): self.emit('error', response.body);
    } else{
      if(response.body.hits.hits.length){
        self.scroll_id = response.body._scroll_id;
        return cb ? cb(null, response.body): self.emit('data', response.body, response.body.hits.hits.length, response.body.hits.hits, response.body.aggregations); 
      } else{
        return cb ? cb(null, {}): self.emit('end');
      }
    }
  });

};

module.exports = es;