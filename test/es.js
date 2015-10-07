"use strict";

var should = require('should');
var request = require('request');

var es = require('../');


describe("Es wrapper Creation Test Suite" ,function () {

  it("Should not create a es wrapper object", function (done) {
  
    var query = {"query":{"filtered":{"query":{"match_all":{}},"filter":{"nested":{"path":"variants","filter":{"bool":{"must":[{"terms":{"variants.category_id":[5030]}}]}}}}}},"size":31,"from":0,"sort":[{"variants.price":{"mode":"min","order":"asc","ignore_unmapped":true}}],"aggs":{"primary_categories":{"terms":{"field":"primary_category_id","size":100,"min_doc_count":1}}}};

    var esOpts = {
    };

    var esObject;

    try {
      esObject = new es(esOpts);
    } catch (e) {
      e.should.not.be.null;
    }
    done();
  });

  it("Should create an es object", function () {
    var url = "http://localhost:9200/catalog/refiner/_search";
  
    var query = {"query":{"filtered":{"query":{"match_all":{}},"filter":{"nested":{"path":"variants","filter":{"bool":{"must":[{"terms":{"variants.category_id":[5030]}}]}}}}}},"size":31,"from":0,"sort":[{"variants.price":{"mode":"min","order":"asc","ignore_unmapped":true}}],"aggs":{"primary_categories":{"terms":{"field":"primary_category_id","size":100,"min_doc_count":1}}}};


    var esOpts = {
      requestOpts : {
        host : "http://localhost:9200"
      }
    };

    var esObject;

    try {
      esObject = new es(esOpts);
    } catch (e) {
      e.should.be.null;
    }
    
  });
  
});

describe("Es wrapper pagination Suite", function () {
  var esObject;
  this.timeout(10000);
  
  var esOptions = {
    requestOpts : {
      host : "http://localhost:9200"
    }
  };

  var query = {"query":{"filtered":{"query":{"match_all":{}},"filter":{"nested":{"path":"variants","filter":{"bool":{"must":[{"terms":{"variants.category_id":[5030]}}]}}}}}},"size":31,"from":0,"sort":[{"variants.price":{"mode":"min","order":"asc","ignore_unmapped":true}}],"aggs":{"primary_categories":{"terms":{"field":"primary_category_id","size":100,"min_doc_count":1}}}};

  var pageSize = 300;
  
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
    esObject.setQuery("catalog/refiner/_search", query);
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

describe("ES wrapper thread pool check", function () {
  it("Should display the thread pool", function (done) {
    var esObject = new es({
      requestOpts : {
	host : "http://localhost:9200"
      }
    });

    esObject._getThreadPoolStatus()
      .then(function (threadPool) {
	threadPool.forEach(function (nodeStat) {
          nodeStat.host.should.not.be.null;
          nodeStat.queueCount.should.be.a.number;
	});
	done();
      })
      .fail(function (error) {
	console.log(error);
      });
  });


});


describe("ES wrapper bulk index ", function () {
  this.timeout(20000);
  var index = "mocha_test";
  after(function (done) {
    var requestOpts = {
      method : "DELETE",
      url : "http://localhost:9200/" + index
    };

    request(requestOpts, function (error , response) {
      console.log(error, response.body);
      done();
    });
  });

  it("Should bulk index", function (done) {
    var esObject = new es({
      requestOpts : {
	host : "http://localhost:9200"
      }
    });

    var data = [];

    function handleError (error) {
      console.log("Error occurred while bulk indexing" , error);
    }

    function proceedAhead() {
      console.log("Its time .....");
      done();
    }

    for (var i = 0 ; i < 40; i ++ ) {
      data.push({
	id : i,
	message : Date.now()
      });
    }

    esObject.bulkIndex("mocha_test", "mocha_type", data, {operation: "update", upsert: true});

    esObject.on('error', handleError);
    esObject.on('done', proceedAhead);

  });
});

