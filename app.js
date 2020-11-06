const {Kafka} = require('kafkajs');
const elasticsearch = require('elasticsearch');
const client = new elasticsearch.Client({
    host: 'localhost:9200'
});

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
});

// For testing purposes so that it always returns all messages upon rerunning the app
const groupNamePrefixGenerator = `${Math.floor(Math.random() * 100)}${Math.floor(Math.random() * 100)}${Math.floor(Math.random() * 100)}`;

const consumer = kafka.consumer({groupId: `${groupNamePrefixGenerator}test-group`});

const consume = async (topic, fromBeginning) => {
    // Consuming
    await consumer.connect();
    await consumer.subscribe({ topic: topic, fromBeginning: fromBeginning });

    await consumer.run({
        eachMessage: async ({topic, partition, message}) => {
            // console.log(message.value.toString());

        }
    })
};

consume('bsocial', true).catch(console.error);
