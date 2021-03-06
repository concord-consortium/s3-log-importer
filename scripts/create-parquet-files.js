const {localConnect, remoteConnect} = require("./shared/db")
const {die} = require("./shared/die")
const {parse} = require("@concord-consortium/hstore-to-json")()
const parquet = require('parquets');
const mkdirp = require('mkdirp')
const Cursor = require("pg-cursor")

const BATCH_SIZE = 1000

const dateRegEx = /^\d\d\d\d-\d\d-\d\d$/

const [_node, _script, ...rest] = process.argv
if (rest.length == 0) {
  die("Usage: npm run pull-logs <START-DATE> [<END-DATE>]")
}
let [startDate, endDate] = rest
endDate = endDate || startDate
if (!dateRegEx.test(startDate) || !dateRegEx.test(endDate)) {
  die("Start and end dates must be in the form YYYY-MM-DD")
}

const startOfDay = (ymd) => `${ymd}T00:00:00.000Z`
const endOfDay = (ymd) => `${ymd}T23:59:59.999Z`
const zeroPad = (n) => n < 10 ? `0${n}` : `${n}`
const dateString = (date) => `${date.getUTCFullYear()}-${zeroPad(date.getUTCMonth() + 1)}-${zeroPad(date.getUTCDate())}`
const s3Path = (date) => `${date.getUTCFullYear()}/${zeroPad(date.getUTCMonth() + 1)}/${zeroPad(date.getUTCDate())}/00`
const toJsonString = (o) => JSON.stringify(parse(o))

const schema = new parquet.ParquetSchema({
  session: {type: "UTF8"},
  username: {type: "UTF8"},
  application: {type: "UTF8"},
  activity: {type: "UTF8"},
  event: {type: "UTF8"},
  event_value: {type: "UTF8"},
  time: {type: "INT_64"},
  parameters: {type: "UTF8"},
  extras: {type: "UTF8"},
  run_remote_endpoint: {type: "UTF8"},
  timestamp: {type: "INT_64"},
});

startDate = startOfDay(startDate)
endDate = endOfDay(endDate)

// adapted from https://github.com/brianc/node-postgres/issues/1839#issuecomment-716840604
const asyncCursor = async function* cursor(client, query, logPrefix) {
  let batchNum = 1
	const cursor = client.query(new Cursor(query.text, query.values));
	try {
		while (true) {
			const rows = await new Promise((resolve, reject) => {
				cursor.read(BATCH_SIZE, (error, rows) => {
          if (error) {
            reject(error)
          } else {
            if (rows.length > 0) {
              console.log(`${logPrefix} ${batchNum++}: read ${rows.length} rows`)
            }
            resolve(rows)
          }
        });
			});
			if (rows.length === 0) break;
			for (const row of rows) {
				yield row;
			}
		}
	} finally {
		cursor.close();
	}
};

const getIds = async (localClient, timeDate, ymd) => {
  const query = {
    text: "SELECT remote_id, extract(epoch from timestamp) as timestamp FROM logs_meta WHERE time >= $1 AND time <= $2",
    values: [startOfDay(ymd), endOfDay(ymd)]
  }

  const timestampMap = {}
  const ids = []

  const rows = asyncCursor(localClient, query, `${ymd} (get ids)`)
	for await (const row of rows) {
    timestampMap[row.remote_id] = row.timestamp
    ids.push(parseInt(row.remote_id, 10))
  }

  return [ids, timestampMap, timeDate, ymd]
}

const getLogData = async (remoteClient, ymd, timeDate, ids, timestampMap) => {
  const subPath = `./output/${s3Path(timeDate)}`
  const outputPath = `${subPath}/${ymd}.parquet`
  if (ids.length == 0) {
    console.log(`Skipping ${outputPath} with ${ids.length} rows`)
    return;
  }
  mkdirp.sync(subPath)
  console.log(`Creating ${outputPath} with ${ids.length} rows`)
  const writer = await parquet.ParquetWriter.openFile(schema, outputPath)

  const query = {
    text: "SELECT id, session, username, application, activity, event, event_value, extract(epoch from time) as time, parameters, extras, run_remote_endpoint FROM logs WHERE id = ANY($1::int[]) ORDER BY id",
    values: [ids]
  }

  const rows = asyncCursor(remoteClient, query, `${ymd} (get log data)`)
  for await (const row of rows) {
    const {id, session, username, application, activity, event, event_value, time, parameters, extras, run_remote_endpoint, timestamp} = row
    await writer.appendRow({
      session: session || "",
      username: username || "",
      application: application || "",
      activity: activity || "",
      event: event || "",
      event_value: event_value || "",
      time: time || 0,
      parameters: toJsonString(row.parameters),
      extras: toJsonString(row.extras),
      run_remote_endpoint: run_remote_endpoint || "",
      timestamp: timestampMap[id] || 0,
    })
  }
  await writer.close()
  console.log(`Created ${outputPath} with ${ids.length} rows`)
}

async function* getAsyncIds(localClient, timeDates) {
  for (const timeDate of timeDates) {
    const ymd = dateString(timeDate)
    yield await getIds(localClient, timeDate, ymd)
  }
}

const getTimeDatesArray = (startDate, endDate) => {
  const dates = []
  const now = new Date(startDate)
  const end = new Date(endDate)
  while (now <= end) {
    dates.push(new Date(now))
    now.setDate(now.getDate()+1)
  }
  return dates;
};

localConnect().then(localClient => {
  remoteConnect().then(async (remoteClient) => {
    const timeDates = getTimeDatesArray(startDate, endDate)
    for await (const [ids, timestampMap, timeDate, ymd] of getAsyncIds(localClient, timeDates)) {
      console.log("************ getLogData ", ymd)
      await getLogData(remoteClient, ymd, timeDate, ids, timestampMap)
    }
  })
  .catch(err => console.error(err))
  .finally(() => {
    console.log("Parquet files created.")
    process.exit()
  })
})