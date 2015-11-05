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
}

es.prototype._getThreadPoolStatus = function() {
  var 
    self          = this,
    deferred      = Q.defer(),
    threadPoolURI = self.host + '/' + "_cat/thread_pool?h=host,bulk.queue",
    responseJson  = [];

  REQUEST(threadPoolURI, function(error, response) {
    if (error) {
      deferred.reject(error);
    }

    if (response.statusCode !== 200) {
      deferred.reject(response.body);
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
    self = this,
    requestOpts;

  //setting params for query
  self.currentPage = self.currentPage || 0;
  self.query.from  = self.from || 0;
  self.query.size  = self.pageSize;

  if(!self.pfx&&!self.pass){
    requestOpts = {
      uri         : self.uri,
      method      : "GET",
      json        : self.query
    };
  } else{
    requestOpts = {
      uri         : self.uri,
      method      : "GET",
      json        : self.query,
      pfx         : require('fs').readFileSync(self.pfx),
      passphrase  : self.pass
    };
  }

  

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
    

    var bulkIndexOptions = {
      method    : "POST",
      url       : self.host + "/_bulk",
      body      : bulkIndexString
    };

     if(self.pfx&&self.pass){    
      bulkIndexOptions.pfx        = require('fs').readFileSync(self.pfx)''
      bulkIndexOptionspassphrase  = self.pass;    
    }

    REQUEST(bulkIndexOptions, function(error, response) {

      if (error) {
        if(cb)
          self.emit('error', error.message,cb);
        else
          self.emit('error', error.message);
      }

      if (response.statusCode !== 200) {
        if(cb)
          self.emit('error', response.body,cb);
        else
          self.emit('error', response.body);
      }
      if(cb)
        self.emit('done',cb);
      else
        self.emit('done');

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
        self.emit('error', new Error("Cannot accept requests for bulk indexing because of high queue count").message);
      } else {
        self.highQueueCount = false;
        _startIndexing();
      }
    })
    .fail(function(error) {
      self.emit('error', error.message);
    });
};

es.prototype.instantiateScroll = function(pageSize){
  var 
    self = this,
    scroll_id,
    requestOpts;

  self.query.size = self.pageSize;

  self.uri = self.uri + '?search_type=scan&scroll=1m';

  if(!self.pfx&&!self.pass){
    requestOpts = {
      uri         : self.uri,
      method      : "GET",
      json        : self.query
    };
  } else{
    requestOpts = {
      uri         : self.uri,
      method      : "GET",
      json        : self.query,
      pfx         : require('fs').readFileSync(self.pfx),
      passphrase  : self.pass
    };
  }

  REQUEST(requestOpts, function(error, response) {
    if (error) {
      self.emit('error', error.message);
    } else if (response.statusCode !== 200) {
      self.emit('error', response.body);
    } else {
      self.scroll_id = response.body._scroll_id;
      self.emit('start');     
    }
  });

};

es.prototype.scroll = function() {
  var
    self        = this,
    scroll_id   = self.scroll_id,
    requestOpts;

  if(!self.pfx&&!self.pass){
    requestOpts = {
      uri         : self.host + '/_search/scroll?scroll=1m&scroll_id=' + scroll_id,  
      method      : "GET",
      json        : true
    };
  } else{
    requestOpts = {
      uri         : self.host + '/_search/scroll?scroll=1m&scroll_id=' + scroll_id,
      method      : "GET",
      pfx         : require('fs').readFileSync(self.pfx),
      passphrase  : self.pass,
      json        : true
    };
  }


    REQUEST(requestOpts,function(error,response){
      if(error){
        self.emit('error', error.message);  
      } else if(response.statusCode !== 200){
        self.emit('error', response.body);
      } else{
        if(response.body.hits.hits.length){
          self.scroll_id = response.body._scroll_id;
          self.emit('data', response.body, response.body.hits.hits.length, response.body.hits.hits, response.body.aggregations);
        } else{
          self.emit('end',response.body);
        }
      }
    });

    
};

module.exports = es;