const {Kafka} = require('kafkajs');
const elasticsearch = require('elasticsearch');
const client = new elasticsearch.Client({
    host: 'localhost:9200'
});

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
});

// For testing purposes so that it always returns all messages upon rerunning the app to fill the Elasticsearch index data
// const groupNamePrefixGenerator = `${Math.floor(Math.random() * 100)}${Math.floor(Math.random() * 100)}${Math.floor(Math.random() * 100)}`;
//
// const consumer = kafka.consumer({groupId: `${groupNamePrefixGenerator}test-group`});

const consumer = kafka.consumer({groupId: 'telemetry2-group'});

// I assume the Elasticsearch index is created already under the name 'bsocial'
/*
PUT bsocial/_doc/1?op_type=create
{"userData":{"firstName":"Test","lastName":"Test","username":"test.test","email":"test.test@gmail.com"},"registrationDate":"2020-11-04 14:50:53"}
 */

const consume = async (topic, fromBeginning) => {
    // Consuming
    await consumer.connect();
    await consumer.subscribe({ topic: topic, fromBeginning: fromBeginning });

    await consumer.run({
        eachMessage: async ({topic, partition, message}) => {
            // console.log(message.value.toString());
            await client.create({
                index: 'bsocial',
                type: '_doc',
                id: message.offset,
                body: JSON.parse(message.value.toString())
            }, (err) => {
                if (err) {
                    console.log(err);
                } else {
                    console.log(`Kafka message ${message.offset} inserted successfully`);
                    // I am using Kibana to test get _search query
                    // GET bsocial/_search
                }
            });
        }
    })
};

consume('bsocial', true).catch(console.error);
