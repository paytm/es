/*
    https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-threadpool.html
    https://www.elastic.co/guide/en/elasticsearch/reference/1.4/cat-thread-pool.html
*/


/* Will get Threadpool status of ES */
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