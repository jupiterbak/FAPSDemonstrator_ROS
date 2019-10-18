const rosnodejs = require('rosnodejs');
rosnodejs.initNode('/my_node')
.then(() => {
  // do stuff
});
