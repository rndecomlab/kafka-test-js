const {
    Kafka
} = require('kafkajs')
const config = require('./config')

const kafka = new Kafka({
    brokers: [config.kafka_server],
    clientId: 'cp-match',
})

const topic = config.kafka_topic
const producer = kafka.producer()

const getRandomNumber = () => Math.round(Math.random(10) * 1000)

function randomDate(start, end) {
    return new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime()));
}

const randomData = num => {
let text =  Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
let dateRandom = randomDate(new Date(2019, 12, 1), new Date()); 
let data = {
    PRODUCTCODE: `00000000002308122${num}`,
    IS_COMBOSET: "N",
    NAME_TH: `วุ้นเส้นผัดต้มยำกุ้งล็อบสเตอร์แคนาดา-${text}`,
    NAME_EN: `Glass Noodles Tom Yum With Canadian Lobster-${text}`,
    ECOMMERCE_PRICE: `36${num}`,
    ECOMMERCE_CLASS_PRICE: "CLSPRA",
    CURRENCY: "THB",
    TAXES_TYPE: `TXTYP01${num}`,
    TAXES_CODE: "TXCOD07",
    TAXES_FLAG: "C",
    EFFECTIVE_DATE: dateRandom,
    SUSPEND_DATE: "2100-01-01T00:00:00+07:00",
    COMBO_FLAG: "Discount"
}
return data
}
const createMessage = num => ({
    key: `key-${num}`,
    value: JSON.stringify(randomData(num)),
})

const sendMessage = () => {
    return producer
        .send({
            topic,
            messages: Array(1000)
                .fill()
                .map(_ => createMessage(getRandomNumber())),
        })
        .then(console.log)
        .catch(e => console.error(`[example/producer] ${e.message}`, e))
}

const run = async () => {
    await producer.connect()
    sendMessage()
}

run().catch(e => console.error(`[example/producer] ${e.message}`, e))
