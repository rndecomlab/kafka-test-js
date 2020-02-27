const {
    Kafka
} = require('kafkajs')
// const fs = require('fs');
const mongoose = require('mongoose')
const config = require('./config')
const Trx = require('./models/trx.model');

mongoose.connect(config.db_url, {
    useNewUrlParser: true,
    useUnifiedTopology: true
})

mongoose.Promise = global.Promise

let db = mongoose.connection

db.on('error', console.error.bind(console, 'MongoDB connection Error!'))

const kafka = new Kafka({
    clientId: 'cp-match',
    brokers: [config.kafka_server],
    // ssl: {
    //     rejectUnauthorized: false,
    //     ca: [fs.readFileSync('cer.crt', 'utf-8')],
    //     cert: fs.readFileSync('ca-cert-sit.pem', 'utf-8'),
    // },
    // sasl: {
    //     mechanism: 'plain',
    //     username: 'matchsit',
    //     password: '4T?_w6k]'
    // },
})

const consumer = kafka.consumer({
    groupId: '1'
})


const consume = async () => {
    await consumer.connect()

    await consumer.subscribe({
        topic: config.kafka_topic,
	fromBeginning: true
	})

    await consumer.run({
        autoCommit: false,
        eachBatch: async ({
            batch,
            resolveOffset,
            heartbeat,
            commitOffsetsIfNecessary
        }) => {
            for (let message of batch.messages) {
                let x = {
                    topic: batch.topic,
                    partition: batch.partition,
                    highWatermark: batch.highWatermark,
                    offset: message.offset,
                    key: message.key.toString(),
                    value: message.value.toString(),
                    headers: message.headers,
                }
                resolveOffset(message.offset)
		await storeData(x)
                await commitOffsetsIfNecessary()
                await heartbeat()
            }
        },
    })
}

const storeData = async (message) => {
    console.log('haha', message)

    let messages = [];

    messages.push({
        value: message.value,
        offset: message.offset,
        partition: message.partition,
        highWaterOffset: message.highWatermark,
        key: message.key
    })

    // let offset = message.offset + 1
    // console.log(offset)
    // if (offset % 3000 === 0 || message.highWatermark - offset === 0) {
        Trx.insertMany(message, function (error, data) {
            if (error) console.log(error)
            return
        });
        // messages = []
    // }
}

consume().catch(e => console.log(e))
