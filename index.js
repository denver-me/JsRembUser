const mqtt = require('mqtt')
const client = mqtt.connect('mqtt://192.168.1.49:1883', {
    username: "denver",
    password: "denver"
})

client.on('connect', () => {
    console.log(`Connected`)
})