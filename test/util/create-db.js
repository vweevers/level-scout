var level = require('level-test')()
  , sublevel = require('level-sublevel/bytewise')
  , index = require('../../').index
  , search = require('../../').search
  , db = sublevel(level('level-scout'), { valueEncoding: 'json' })
  , n = 0

require('./tape-debug')

module.exports = function (installIndex, installSearch) {
  var sdb = db.sublevel(++n)

  if (installIndex) index.install(sdb)
  if (installSearch) search.install(sdb)

  return sdb
}
