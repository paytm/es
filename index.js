/*jshint multistr: true ,node: true */
"use strict";

var
    /* NODE INTERNAL */
    UTIL            = require('util'),
    PATH            = require('path'),
    EVENTEMITTER    = require('events').EventEmitter,

    /* NPM Third Party */
    _               = require('lodash'),
    Q               = require('q'),
    REQUEST         = require('request'),

    /* Internal */
    LIB             = require('./lib');


UTIL.inherits(es, EVENTEMITTER);

function es(opts) {
  /*
   * The opts must contain requestOpts  
   */

    var self = this;
    EVENTEMITTER.call(self);

    /*
        Lets save opts. main expected opts are requestOpts which are required for connectivity
    */
    self.opts = opts;
}

module.exports = es;