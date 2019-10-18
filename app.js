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
		// Create ROS publisher on the 'faps_demo' topic with String message
		let pub = rosNode.advertise('/faps_demo', std_msgs.String);

		var sendROSMessage = function(msg){
			const ros_msg = new std_msgs.String();
			ros_msg.data = "" + msg;
			// Send it to ROS
			// Publish over ROS
			pub.publish(ros_msg);
		};

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
				amqp_ch.assertExchange("FAPS_DEMONSTRATOR_OrderManagement_Orders", 'fanout', {durable: false});
				amqp_ch.assertExchange("FAPS_DEMONSTRATOR_LiveStreamData_ConveyorData", 'fanout', {durable: false});
				
				// Queue for "new orders"
				amqp_ch.assertQueue('FAPS_DEMONSTRATOR_OrderManagement_Orders_ROS', {exclusive: false, durable: false, autoDelete:true}, function(err, q) {
					if (err){
						console.log('AMQP Queue Assertion Error: ' + err.toString());
					}else{
						console.log(" [*] Waiting for messages in %s. To exit press CTRL+C", q.queue);
						ch.bindQueue(q.queue, "FAPS_DEMONSTRATOR_OrderManagement_Orders", '');			
						ch.consume(q.queue, function(msg) {
							_obj = JSON.parse(msg.content.toString());
							var obj_to_send = {
								title:'Neuer Auftrag',
								payload: _obj
							};

							sendROSMessage(JSON.stringify(obj_to_send));
							// Log through stdout and /rosout
							rosnodejs.log.info('FAPS send: [Neuer Auftrag]');

						}, {noAck: true});
					}
				});

				// Queue for 'Produkt_wartet_auf_Abgabe' and 'Product abgegeben'
				var last_Produkt_wartet_auf_Abgabe = null;
				var last_Product_abgegeben = null;
				amqp_ch.assertQueue('FAPS_DEMONSTRATOR_LiveStreamData_ConveyorData', {exclusive: false, durable: false, autoDelete:true}, function(err, q) {
					if (err){
						console.log('AMQP Queue Assertion Error: ' + err.toString());
					}else{
						console.log(" [*] Waiting for messages in %s. To exit press CTRL+C", q.queue);
						ch.bindQueue(q.queue, "FAPS_DEMONSTRATOR_LiveStreamData_ConveyorData", '');			
						ch.consume(q.queue, function(msg) {
							_obj = JSON.parse(msg.content.toString());
							if (last_Produkt_wartet_auf_Abgabe == null){
								last_Produkt_wartet_auf_Abgabe = _obj.value.DB33.Produkt_wartet_auf_Abgabe;
							}
							if (last_Product_abgegeben == null){
								last_Product_abgegeben = _obj.value.DB33.Product_abgegeben;
							}

							// Product wartet auf abgabe
							if (last_Produkt_wartet_auf_Abgabe === 0 && _obj.value.DB33.Produkt_wartet_auf_Abgabe === 1){
								// Steigende Flanke nachricht schicken.
								var obj_to_send = {
									title:'Product Wartet auf Abgabe',
									payload: {
										timestamp: Date.now()
									}
								};
	
								sendROSMessage(JSON.stringify(obj_to_send));
								// Log through stdout and /rosout
								rosnodejs.log.info('FAPS send: [Product Wartet auf Abgabe.]');
							}
							last_Produkt_wartet_auf_Abgabe = _obj.value.DB33.Produkt_wartet_auf_Abgabe;

							// Product abgegeben
							if (last_Product_abgegeben === 0 && _obj.value.DB33.Product_abgegeben === 1){
								// Steigende Flanke nachricht schicken.
								var obj_to_send = {
									title:'Product an den AGV abgegeben.',
									payload: {
										timestamp: Date.now()
									}
								};
	
								sendROSMessage(JSON.stringify(obj_to_send));
								// Log through stdout and /rosout
								rosnodejs.log.info('FAPS send: [Product an den AGV abgegeben.]');
							}
							last_Product_abgegeben = _obj.value.DB33.Product_abgegeben;	
						}, {noAck: true});
					}
				});

				// Publisher for AGV
				amqp_ch.assertExchange("FAPS_DEMONSTRATOR_Conveyor_DataFromCloud", 'fanout', {durable: false},function(err, q) {
					if (err){
						console.log('AMQP Exchange for message of the AGV could not be found: ' + err.toString());
					}else{
						// Start the listener
						let sub = rosNode.subscribe('/faps_demo', std_msgs.String, (data) => { 
							// define callback execution
							rosnodejs.log.info('I heard: [' + data.data + ']');

							var msgObj = JSON.stringify(data.data);
							if(msgObj.title){
								if(msgObj.title === "AGV_bereit"){
									var obj_to_pub = {
										"AGV_bereit": 1
									}
									// Send Signal to Demonstrator
									amqp_ch.publish(FAPS_DEMONSTRATOR_Conveyor_DataFromCloud, '', Buffer.from(JSON.stringify(obj_to_pub)));

									// Wait for 2 seconds and reset the signal
									setTimeout(function() {
										obj_to_pub.AGV_bereit = 0;
										// Send Signal to Demonstrator
										amqp_ch.publish(FAPS_DEMONSTRATOR_Conveyor_DataFromCloud, '', Buffer.from(JSON.stringify(obj_to_pub)));
									}, 2000);
								}
							}
						});
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







