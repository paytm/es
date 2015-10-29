"use strict";

var es = require('../lib/es.js');

// Custom Query Set. Will be used for scan and scroll
var scanIndex = '';
var scanType = '';


var query = {"query":{"filtered":{"query":{"match_all":{}},"filter":{"nested":{"path":"variants","filter":{"bool":{"must":[{"terms":{"variants.category_id":["5030"]}}]}}}}}}};

var esScanClient = new es({
  requestOpts : {
    // To be given by User
    host : "http://"
  }
});


var esBulkClient = new es({
  requestOpts : {
    host : "http://pawslmktnginx01/dawses/"
  }
});


function indexData (data){
  // Format data here
  var formattedData = [];

  data.hits.hits.forEach(function (x) {
    formattedData.push({
      "id" : x._id,
      "data" : x._source
    });
  });

  esScanClient.scroll();
  esBulkClient.bulkIndex(scanIndex, scanType,formattedData, {operation : "update", upsert : true});
}

esScanClient.setQuery([scanIndex, scanType, "_search"].join('/'), query);
esScanClient.setPageSize(5000);
esScanClient.instantiateScroll();



esBulkClient.on('error', function (error) {
  console.log("Error occured on bulk index ", error);
  process.exit(1);
});

esBulkClient.on('done', function () {
  esScanClient.instantiateScroll();
});


esScanClient.on('data', indexData);

esScanClient.on('error', function (error) {
  console.log("Search and Scan error ", error);
  process.exit(1);
});


esScanClient.on('start', function(){
  esScanClient.scroll();
});



esScanClient.on('end', function () {
  console.log("Ended Scan");
  process.exit(0);
});
