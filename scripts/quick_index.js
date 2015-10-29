"use strict";

var es = require('../lib/es.js');

// Custom Query Set. Will be used for scan and scroll
var scanIndex = '';
var scanType = '';

// Custom json for scan and scroll
var query = '';

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
  esScanClient.scroll();
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
