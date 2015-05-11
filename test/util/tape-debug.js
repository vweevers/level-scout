var format  = require('util').format
  , debug   = module.parent.require('debug')
  , results = module.parent.require('tape/lib/results')
  , push    = results.prototype.push

// Ugly hack to queue debug() output in tape's output stream,
// to retain order. This will be removed once the debug
// parts are actually unit tested.
results.prototype.push = function (t) {
  debug.log = function() {
    this._stream.queue( format.apply(null, arguments).trim() + '\n' )
  }.bind(this)

  // Restore original function
  this.push = push
  this.push(t)
}

module.exports = debug
