require('dotenv').config()
const mqtt = require('mqtt')
const admin = require('firebase-admin')
const serviceAccount = require(`./${process.env.nameOfServiceFile}`)
const dayJs = require("dayjs")
const customParseFormat = require("dayjs/plugin/customParseFormat")
dayJs.extend(customParseFormat)
admin.initializeApp({ credential: admin.credential.cert(serviceAccount) })
const firestore = admin.firestore()
let schedulesRef = firestore.collection("schedules")

console.log(`username: ${process.env.mqttUsername}\npassword: ${process.env.mqttPassword}`)
const client = mqtt.connect('mqtt://ec2-54-79-234-120.ap-southeast-2.compute.amazonaws.com:1883', { username: process.env.mqttUsername, password: process.env.mqttPassword })
const clientMap = new Map()
clientMap.set("esp101_301", false)

const db = require('better-sqlite3')('schedules.db', {});

db.prepare(`
    CREATE TABLE IF NOT EXISTS Schedule (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      RoomId VARCHAR(55) NOT NULL
    )
  `).run();

db.prepare(`
    CREATE TABLE IF NOT EXISTS WeekSchedule (
      weekId INTEGER PRIMARY KEY AUTOINCREMENT,
      scheduleId BIGINT NOT NULL,
      FOREIGN KEY (scheduleId) REFERENCES Schedule(id)
    )
  `).run();

db.prepare(`
    CREATE TABLE IF NOT EXISTS DaySchedule (
      dayId INTEGER PRIMARY KEY AUTOINCREMENT,
      weekId BIGINT NOT NULL,
      dayName TEXT NOT NULL,
      FOREIGN KEY (weekId) REFERENCES WeekSchedule(weekId)
    )
  `).run();

db.prepare(`
    CREATE TABLE IF NOT EXISTS TimeSchedule (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      start_time TEXT NOT NULL,
      end_time TEXT NOT NULL,
      dayId INTEGER NOT NULL,
      FOREIGN KEY (dayId) REFERENCES DaySchedule(dayId)
    )
  `).run();
db.prepare(`CREATE TABLE IF NOT EXISTS TurnOffQueue (
    statusId INTEGER PRIMARY KEY AUTOINCREMENT,
    RoomId VARCHAR(50) NOT NULL,
    end_time VARCHAR(50) NOT NULL);`).run()

let insertScheduleStmt = db.prepare(`INSERT INTO Schedule (RoomID) VALUES (?) RETURNING id`)
let insertScheduleWeek = db.prepare(`INSERT INTO WeekSchedule (scheduleId) VALUES (?) RETURNING weekId`)
let insertScheduleDay = db.prepare(`INSERT INTO DaySchedule (weekId, dayName) VALUES (?, ?) RETURNING dayId`)
let insertTimeSchedule = db.prepare(`INSERT INTO TimeSchedule (start_time, end_time, dayId) VALUES (?, ?, ?)`)
let turnOffQueue = db.prepare(`INSERT INTO TurnOffQueue (RoomId, end_time) VALUES (?, ?);`)
let checkIfARoomAndEndTimeIsInQueue = db.prepare(`SELECT * FROM TurnOffQueue WHERE end_time=?;`)
let checkIfARoomIsInQueueToTurnOff = db.prepare(`SELECT EXISTS(SELECT * FROM TurnOffQueue WHERE RoomId=?) AS DoesExist`)
let checkIfAnEndTimeIsReaced = db.prepare(`SELECT * FROM TurnOffQueue WHERE end_time=?;`)
// let selectAllTimeMatched = db.prepare(`SELECT 1 FROM TimeSchedule INNER JOIN WHERE `)
let deleteFromQueue = db.prepare(`DELETE FROM TurnOffQueue WHERE RoomId=? AND end_time=?`)

let selectMatchingTime = db.prepare(`
  SELECT Schedule.RoomId, TimeSchedule.end_time FROM Schedule 
  INNER JOIN WeekSchedule 
  ON Schedule.id = WeekSchedule.scheduleId 
  INNER JOIN DaySchedule 
  ON WeekSchedule.weekId = DaySchedule.weekId 
  INNER JOIN TimeSchedule 
  ON DaySchedule.dayId = TimeSchedule.dayId
  WHERE DaySchedule.dayName=? AND TimeSchedule.start_time=?;`)

client.on('connect', () => {
  console.log(`Connected`)
  client.subscribe("node_disconnected")
  client.subscribe("collect_schedule")
})

client.on("message", (topic, message) => {
  console.log(`A Message arrived from ${topic}`)
  if (topic == "collect_schedule") {

    console.log(`Schedule was uploaded, burning previous schedule.`)

    db.exec(`DELETE FROM TurnOffQueue;`)
    db.exec(`DELETE FROM TimeSchedule;`)
    db.exec(`DELETE FROM DaySchedule;`)
    db.exec(`DELETE FROM WeekSchedule;`)
    db.exec(`DELETE FROM Schedule;`)

    console.log("Cleared")

    schedulesRef.get().then((querySnapshot) => {
      console.log('Fetching FirebaseFirestore Data')
      querySnapshot.forEach(async (docSnap) => {
        let doc = await schedulesRef.doc(docSnap.id).get()
        console.log('Fetched')
        let scheduleData = doc.data()
        let scheduleMap = scheduleData.scheduleOfDayMap

        console.dir(scheduleMap)

        let scheduleId = insertScheduleStmt.get(scheduleData.roomId)
        console.dir(scheduleId)

        console.log(`Schedule Entry Inserted with id ${scheduleId.id}`)
        let scheduleWeekId = insertScheduleWeek.get(scheduleId.id)
        console.log(`Week Inserted with id ${scheduleWeekId.weekId}`)

        Object.keys(scheduleMap).forEach((key) => {
          console.log(`Day Is ${key}`)
          let scheduleDayId = insertScheduleDay.get([scheduleWeekId.weekId, key])
          scheduleMap[key].hours.forEach(e => {
            console.dir(e.startTime)
            const start_time = dayJs(`${e.startTime}`, 'hh:mma').format("HH:mm")
            const end_time = dayJs(`${e.endTime}`, 'hh:mma').format("HH:mm")
            console.log(`Start Time: ${start_time} \n End Time: ${end_time}`)
            insertTimeSchedule.run([start_time, end_time, scheduleDayId.dayId])
          })
        })
      })
    })
  }
})

setInterval(() => {
  let current = dayJs().locale('tl-ph').format('HH:mm')
  let currentDate = dayJs().locale('th-ph').format('dddd')
  let matchesForStartTime = selectMatchingTime.all(currentDate, current)
 

  if (matchesForStartTime.length > 0) {
    matchesForStartTime.forEach(result => {
      console.log(result)
      let isRoomInQueue = checkIfARoomIsInQueueToTurnOff.get(result.RoomId)
      console.log(`Is Room in Queue ${isRoomInQueue}`)
      console.dir(isRoomInQueue)
      let roomId = result.RoomId + ""
      if (isRoomInQueue.DoesExist <= 0) {
        client.publish(`turn_on/${roomId.toLowerCase()}`, "1")
        console.log(`A match was found at room ${result.RoomId} and it ends at ${result.end_time}`)
        turnOffQueue.run(roomId, result.end_time)
      }
    })
  }

  let scheduledTurnOff = checkIfAnEndTimeIsReaced.all(current)

  if (scheduledTurnOff.length > 0) {
    console.dir(scheduledTurnOff)
    scheduledTurnOff.forEach(item => {
      let roomId = item.RoomId + ""
      client.publish(`turn_off/${roomId.toLowerCase()}`, "0")
      console.log(`A schedule for turn off is reached ${roomId}`)
      deleteFromQueue.run(item.RoomId, current)
    })
  }

  console.log(`Date: ${currentDate} Time: ${current}`)

}, 1000)

function timeToSeconds(time) {
  const [hours, minutes] = time.split(":").map(Number);
  return hours * 3600 + minutes * 60;
}


