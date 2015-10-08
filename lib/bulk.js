/*
    https://www.elastic.co/guide/en/elasticsearch/guide/current/bulk.html
    https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
*/

es.prototype.bulkIndex = function(index, type, data, opts) {

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

    var bulkIndexOptions = {
      method    : "POST",
      url       : self.host + "/_bulk",
      body      : bulkIndexString
    };

    REQUEST(bulkIndexOptions, function(error, response) {

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