#!/usr/bin/env node
var amqp = require('amqplib/callback_api');
var JSONStream = require('pixl-json-stream');

// setup stdin / stdout streams 
process.stdin.setEncoding('utf8');
process.stdout.setEncoding('utf8');

var stream = new JSONStream( process.stdin, process.stdout );
stream.on('json', function (job) {

    var params = job.params
    console.log("params", params)

    amqp.connect('amqp://rabbitmq', function(error0, connection) {
        if (error0) {
            throw error0;
        }
        connection.createChannel(function(error1, channel) {
            if (error1) {
                throw error1;
            }
            var queue = params.queue;
            try {
                var msg = JSON.parse(params.message);        
                console.log("msg", msg)
                channel.assertQueue(queue, {
                    durable: true
                });
    
                channel.sendToQueue(queue, Buffer.from(JSON.stringify(msg)));
                console.log(" [x] Sent %s", msg);
                
                stream.write({complete:1}); 
                process.stdout.write(JSON.stringify({ complete: 1 }) + "\n");

                setTimeout(function () {
                    console.log("Closing");
                    connection.close();
                    process.exit(0)
                }, 500);

            } catch (error2) {
                throw error2;
            }
        });
    });
})
