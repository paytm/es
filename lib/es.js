/*jshint multistr: true ,node: true */
"use strict";

var 
  UTIL           = require('util'),
  EVENTEMITTER   = require('events').EventEmitter,
  _              = require('lodash'),
  REQUEST        = require('request'),
  L              = require('lgr');

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
}

es.prototype.paginate = function (pageSize) {
  var self = this;

  //setting params for query
  self.currentPage  = self.currentPage || 0;
  self.query.from   = self.from || 0;
  self.query.size   = self.pageSize;

  var requestOpts = {
    uri    : self.uri,
    method : "GET",
    json   : self.query
  };

  REQUEST(requestOpts, function (error, response) {
    if (error) {
      self.emit('error', error);
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

module.exports = es;
