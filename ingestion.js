var aiota = require("aiota-utils");
var express = require("express");
var cookieParser = require("cookie-parser");
var methodOverride = require("method-override");
var amqp = require("amqp");
var http = require("http");
var MongoClient = require("mongodb").MongoClient;
var config = require("./config");
var rpc = require("amqp-rpc").factory({ url: "amqp://" + config.amqp.login + ":" + config.amqp.password + "@" + config.amqp.host + ":" + config.amqp.port });

var bus = amqp.createConnection(config.amqp);

var db = null;
var queues = { "register-queue": false, "session-queue": false, "response-queue": false, "telemetry-queue": false };

bus.on("ready", function() {
	for (var q in queues) {
		bus.queue(q, { autoDelete: false, durable: true }, function(queue) {
			queues[queue.name] = true;
		});
	}
});

function sendPOSTResponse(response, data)
{
	response.contentType("json");
	response.send(data);
}

function bodyParser(request, response, next)
{
	if (request._body) {
		next();
		return;
	}

	if (request.method == "POST") {
		response.setHeader("Access-Control-Allow-Origin", "*");
	}
	
	request.body = request.body || {};
	
	// Check Content-Type
	var str = request.headers["content-type"] || "";
	var contentType = str.split(';')[0];
  
  	if (contentType != "text/plain") {
		return next();
	}
	
	// Flag as parsed
	request._body = true;
	
	var buf = "";
	
	request.setEncoding("utf8");
	
	request.on("data", function (chunk) {
		buf += chunk
	});
	
	request.on("end", function () {	
		try {
			request.body = JSON.parse(buf);
			next();
		}
		catch (err) {
			err.body = buf;
			err.status = 400;
			next(err);
		}
	});
}

var app = express();

app.use(cookieParser());
app.use(bodyParser);
app.use(methodOverride());
app.use(express.static(__dirname + "/public"));

// POST requests
app.post("/v1", function(request, response) {
	aiota.validate(db, request.body, function(err, ack, obj) {
		if (err) {
			sendPOSTResponse(response, ack);
		}
		else {
			var queue = aiota.getQueue(request.body.header.class);
			
			if (request.body.header.class.group == "longpolling") {
				// Make an RPC call on the message queue
				rpc.call(queue, obj, function(result) {
					response.send(result);
				});
			}
			else {
				// Publish obj on the message queue
				if (queues.hasOwnProperty(queue)) {
					if (queues[queue]) {
						sendPOSTResponse(response, ack);
						bus.publish(queue, obj, { deliveryMode: 2 });
					}
					else {
						sendPOSTResponse(response, { error: "The '" + queue + "' queue is not ready for use.", errorCode: 300002 });
					}
				}
				else {
					sendPOSTResponse(response, { error: "Invalid queue.", errorCode: 300001 });
				}
			}
		};
	});
});

MongoClient.connect("mongodb://" + config.aiotaDatabase.host + ":" + config.aiotaDatabase.port + "/" + config.aiotaDatabase.name, function(err, aiotaDB) {
	if (err) {
		aiota.log(config.processName, config.serverName, null, err);
	}
	else {
		MongoClient.connect("mongodb://" + config.appDatabase.host + ":" + config.appDatabase.port + "/" + config.appDatabase.name, function(err, dbConnection) {
			if (err) {
				aiota.log(config.processName, config.serverName, aiotaDB, err);
			}
			else {
				db = dbConnection;
				http.createServer(app).listen(config.port);
		
				setInterval(function() { aiota.heartbeat(config.processName, config.serverName, aiotaDB); }, 10000);

				process.on("SIGTERM", function() {
					aiota.terminateProcess(config.processName, config.serverName, db, function() {
						process.exit(1);
					});
				});
			}
		});
	}
});
