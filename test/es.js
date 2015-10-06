"use strict";

var should = require('should');
var requestor = require('requestor');

var es = require('../');


describe("Es wrapper Creation Test Suite" ,function () {

  it("Should not create a es wrapper object", function (done) {
  
    var query = {"query":{"filtered":{"query":{"match_all":{}},"filter":{"nested":{"path":"variants","filter":{"bool":{"must":[{"terms":{"variants.category_id":[5030]}}]}}}}}},"size":31,"from":0,"sort":[{"variants.price":{"mode":"min","order":"asc","ignore_unmapped":true}}],"aggs":{"primary_categories":{"terms":{"field":"primary_category_id","size":100,"min_doc_count":1}}}};

    var cursorOpts = {
    };

    var esObject;

    try {
      esObject = new es(cursorOpts);
    } catch (e) {
      e.should.not.be.null;
    }
    done();
  });

  it("Should create an es object", function () {
    var url = "http://localhost:9200/catalog/refiner/_search";
  
    var query = {"query":{"filtered":{"query":{"match_all":{}},"filter":{"nested":{"path":"variants","filter":{"bool":{"must":[{"terms":{"variants.category_id":[5030]}}]}}}}}},"size":31,"from":0,"sort":[{"variants.price":{"mode":"min","order":"asc","ignore_unmapped":true}}],"aggs":{"primary_categories":{"terms":{"field":"primary_category_id","size":100,"min_doc_count":1}}}};

    var requestorOpts = {
      maxSockets          : 100,
      minSockets          : 10,
      timeout             : 60000,
      reHitTimedOut       : false,
      maxPendingCount     : 10000,
      debugLog            : false
    };

    var requestorObject = new requestor(requestorOpts);

    var cursorOpts = {
      host : "http://localhost:9200",
    };

    var esObject;

    try {
      esObject = new es(cursorOpts);
    } catch (e) {
      e.should.be.null;
    }
    
  });
  
});


describe("Es wrapper pagination Suite", function () {
  var esObject;
  this.timeout(10000);
  
  var esOptions = {
    host : "http://localhost:9200",
    index : "catalog",
    type : "refiner"
  };

  var query = {"query":{"filtered":{"query":{"match_all":{}},"filter":{"nested":{"path":"variants","filter":{"bool":{"must":[{"terms":{"variants.category_id":[5030]}}]}}}}}},"size":31,"from":0,"sort":[{"variants.price":{"mode":"min","order":"asc","ignore_unmapped":true}}],"aggs":{"primary_categories":{"terms":{"field":"primary_category_id","size":100,"min_doc_count":1}}}};

  var pageSize = 30;
  
  it("Should not paginate", function (done) {
    esObject = new es(esOptions);
    try {
      esObject.setQuery(query);
    } catch (e) {
      e.should.not.be.null;
    }

    done();
    
  });
  
  it("Should paginate", function (done) {
    esObject = new es(esOptions);
    esObject.setQuery("catalog", "refiner", query);
    esObject.setPageSize(pageSize);

    var hasMore = true;
    var hits  = [];

    function processData (data) {
      esObject.paginate();
    }

    function handleError (error) {
      console.log("Failed . Test case should fail");
    }

    function proceedAhead () {
      console.log("Be done with ES object , continue");
      done();
    }

    esObject.paginate();
    esObject.on('data', processData);
    esObject.on('error', handleError);
    esObject.on('end', proceedAhead);
  });
});

