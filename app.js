const rosnodejs = require('rosnodejs');
const amqp = require('amqplib/callback_api');
var amqp_connection = null;
var amqp_ch = null;

// Requires the std_msgs message package
const std_msgs = rosnodejs.require('std_msgs').msg;



function talker() {
  // Register node with ROS master
  rosnodejs.initNode('/talker_node')
    .then((rosNode) => {
      // Create ROS publisher on the 'chatter' topic with String message
      let pub = rosNode.advertise('/chatter', std_msgs.String);
 	console.log("Start AMQP Client.");

	/*#####################################################################################*/
	/* AMQP CLIENT
	/*#####################################################################################*/
	// connect to brocker
	amqp.connect("amqp://esys:esys@cloud.faps.uni-erlangen.de",function(err, conn) {
	    if (err != null) {
		console.log('AMQP Connection Error: ' + err.toString());
		return;
	    }
	    amqp_connection = conn;

	    amqp_connection.on('error', function(err) {
		console.log("AMQP Generated event 'error': " + err);
	    });

	    amqp_connection.on('close', function() {
		console.log("AMQP Connection closed.");
		process.exit();
	    });
	    amqp_connection.createChannel(function(err, ch) {
		if (err != null) {
		    console.log('AMQP Chanel Error: ' + error.toString());
		    return;
		}

		amqp_ch = ch;
		amqp_ch.assertExchange("FAPS_DEMONSTRATOR_ImageProcessing_ProcessingSignals", 'fanout', {durable: false});

		// test client
		amqp_ch.assertQueue('FAPS_DEMONSTRATOR_ImageProcessing_ProcessingSignals_ROS', {exclusive: true}, function(err, q) {
		    if (err){
			console.log('AMQP Queue Assertion Error: ' + err.toString());
		    }else{
			console.log(" [*] Waiting for messages in %s. To exit press CTRL+C", q.queue);
			ch.bindQueue(q.queue, "FAPS_DEMONSTRATOR_ImageProcessing_ProcessingSignals", '');

			
			ch.consume(q.queue, function(msg) {
			    console.log(" [x] %s", msg.content.toString());
			    _obj = JSON.parse(msg.content.toString());

			    var energy_data = Math.abs(_obj.value.data.Portal_Wirkleistung_L1);
			    const ros_msg = new std_msgs.String();
		            ros_msg.data = msg.content.toString();

			    // Send it to ROS
			    // Publish over ROS
			    pub.publish(ros_msg);
			    // Log through stdout and /rosout
			    rosnodejs.log.info('FAPS send: [' + ros_msg.data + ']');

			}, {noAck: true});
		    }
		});

	    });
	});

    });
}

if (require.main === module) {
  // Invoke Main Talker Function
  talker();
}







