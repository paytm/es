/*
    Scan and scroll present some problems in pagination so we go for normal procedure of "from" and "size"
*/
"use strict";

var
    /* NODE INTERNAL */
    PATH            = require('path'),

    /* NPM Third Party */
    _               = require('lodash'),
    Q               = require('q');


module.exports = {

    /* Lets fill the space left by requst opts */
    setPaginateOpts : function(uri, queryBody, pageSize) {
        var
            self = this;

        if(uri)
            _.set(self.opts.requestOpts.uri, PATH.join(_.get(self.opts.requestOpts.uri, ''), uri));
        
        if(queryBody)
            _.set(self.opts.requestOpts.json, queryBody);

        self.opts.paginateOpts = {
            pageSize : pageSize,
        };
    },


    /* Paginate call to get next page */
    paginate    : function(pageSize) {
        var self = this;

        //setting params for query
        self.currentPage = self.currentPage || 0;
        self.query.from  = self.from || 0;
        self.query.size  = self.pageSize;

      var requestOpts = {
        uri         : self.uri,
        method      : "GET",
        json        : self.query,
        pfx         : require('fs').readFileSync(self.pfx),
        passphrase  : self.pass
      };

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

};