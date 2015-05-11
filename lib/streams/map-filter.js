var range = require('../util/range')
  , through2 = require('through2').obj

module.exports = function (mapFn, keyRange, subset){
  var match = range.matcher(range.normalize(subset.length, keyRange))

  return through2(function(kv, _, next){
    var indexKey = mapFn(kv.key, kv.value, subset)

    if (indexKey != null && match(indexKey)) next(null, kv)
    else next()
  })
}
