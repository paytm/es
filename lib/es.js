/*jshint multistr: true ,node: true */
"use strict";

var UTIL = require('util');
var EVENTEMITTER = require('events').EventEmitter;
var REQUESTOR = require('requestor');
// SAMARTH : have to remove request module depenency as soon
// as the requestor fix is made.
var REQUEST = require('request');
var L = require('lgr');

/*
 * The idea is to bind a query to one of the esCursor objects
 * and give it a pageSize parameter.
 */

UTIL.inherits(es, EVENTEMITTER);

function es (opts) {
  /*
   * The opts will contain three keys

   * host : The host of the elastic search instance to which the call has to be made.

   * index : The index of the elastic search.

   * type : The mapping from which data is supposed to be fetched.

   */
  
  var self = this;

  EVENTEMITTER.call(self);

  var mandatoryFields = ['host'];
  var missingFields = [];
  var error = false;


  var requestorOpts = {
    maxSockets          : 100,
    minSockets          : 10,
    timeout             : 60000,
    reHitTimedOut       : false,
    maxPendingCount     : 10000,
    debugLog            : true
  };
  

  mandatoryFields.forEach(function (field) {
    if (opts[field] === undefined) {
      missingFields.push(field);
    }
  });

  if (missingFields.length) {
    throw new Error("Provide " + missingFields.join(',') + " to initialize the cursor successfully");
  }
  
  self.host = opts.host;
  self.requestor = new REQUESTOR(requestorOpts);
}

es.prototype.paginate = function (pageSize) {
  var self = this;

  self.currentPage = self.currentPage || 0;

  /*
   FOR REFERENCE DELETE LATER :
    * Third argument is Callback (error, response, body, response.statusCode, response.headers)
   */

  self.query.from = self.from || 0;
  self.query.size = self.pageSize;

  var requestOpts = {
    uri : [self.host,self.index, self.type, '_search'].join('/'),
    method : "GET",
    json: self.query
  };

  REQUEST(requestOpts, function (error, response) {
    /*
     Not able to understand the issue with requestor , will have to debug this separately.
     */
    
  // self.requestor.hit(requestOpts, {}, function (error , response) {
    if (error) {
      self.emit('error', error);
    } else if (response.statusCode !== 200) {
      self.emit('error', response.body);
    } else {
      
      self.from = (++self.currentPage * self.pageSize) + 1;

      if (response.body.hits && response.body.hits.hits.length) {
	// Just returning the docs as of now.
	// Will also handle the case where one might require aggregations
	self.emit('data', response.body.hits.hits);
      } else {
	self.emit('end' , response.body);
      }
    }
    
  });
  
};

es.prototype.setQuery = function (index, type, query) {
  var self = this;

  if (!index) {
    throw new Error ("Please provide an index to query upon");
  }

  if (!type) {
    throw new Error ("Please provide a type to query upon");
  }

  if (!query) {
    throw new Error("Please provide a query to paginate upon");
  }
  
  self.index = index;
  self.type = type;
  self.query = query;

};

es.prototype.setPageSize = function (pageSize) {
  var self = this;
  self.pageSize = pageSize;
};

module.exports = es;
