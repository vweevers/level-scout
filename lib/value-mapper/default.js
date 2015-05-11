var deep = require('deep-dot')

module.exports = function defaultMap(key, value, props) {
  return props.map(deep.bind(null, value))
}
