var queryFilter = require('./lib/streams/query-filter')

module.exports = function filter(db, query, opts) {
  return db.createReadStream()
    .pipe(queryFilter(query, opts))
}

module.exports.install = function(db) {
  db.createFilterStream = module.exports.bind(null, db)
}
