exports.add = function(tree, index) {
  var props = index.properties, node = tree

  for(var i=0, l=props.length; i<l; i++) {
    node = node[props[i]] || (node[props[i]] = Object.create(null))
  }

  node.__index = index
}

exports.create = function(indexes) {
  var tree = Object.create(null)

  if (indexes != null) {
    for(var name in indexes) {
      exports.add(tree, indexes[name])
    }
  }

  return tree
}
