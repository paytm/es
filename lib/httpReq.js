/*
    Just to 
*/
"use strict";

var
    /* NODE INTERNAL */
    PATH            = require('path'),

    /* NPM Third Party */
    _               = require('lodash'),
    Q               = require('q'),
    REQUEST         = require('request');


module.exports = {

    /* HTTP Request */
    req    : function(requestOpts, cb) {

      REQUEST(requestOpts, function(error, response) {
        return cb(error, response);

        /* TODO we should parse error here only */
      });

    },
};