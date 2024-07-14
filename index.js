const express = require('express');
const { Kafka, logLevel } = require('kafkajs');
const app = express();
const port = 8080;

const kafka = new Kafka({
  clientId: 'seroter-consumer',
  brokers: ['bootstrap.seroter-kafka.us-west1.managedkafka.seroter-project-base.cloud.goog:9092'],
  ssl: {
    rejectUnauthorized: false
  },
  logLevel: logLevel.DEBUG,
  sasl: {
    mechanism: 'plain', // scram-sha-256 or scram-sha-512
    username: 'seroter-bq-kafka@seroter-project-base.iam.gserviceaccount.com',
    password: '...'
  },
});

const consumer = kafka.consumer({ groupId: 'message-retrieval-group' });

//create variable that holds an array of "messages" that are strings
let messages = [];

async function run() {
  await consumer.connect();
  //provide topic name when subscribing
  await consumer.subscribe({ topic: 'topic1', fromBeginning: true }); 

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(`################# Received message: ${message.value.toString()} from topic: ${topic}`);
      //add message to local array
      messages.push(message.value.toString());
    },
  });
}

app.get('/consume', (req, res) => {
    //return the array of messages consumed thus far
    res.send(messages);
});

run().catch(console.error);

app.listen(port, () => {
  console.log(`App listening at http://localhost:${port}`);
});
