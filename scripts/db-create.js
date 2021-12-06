const {localConnect} = require("./shared/db")

const checkIfDbExists = (client) => {
  const query = {
    text: 'SELECT COUNT(datname) FROM pg_database WHERE datname = $1',
    values: [process.env.LOCAL_DB_DATABASE],
    rowMode: 'array',
  }
  console.log(`Checking if ${process.env.LOCAL_DB_DATABASE} database exists...`)
  return client.query(query)
    .then((result) => result.rows[0][0] !== "0")
}

const createDb = (client) => {
  console.log("Creating database...")
  return client.query(`CREATE DATABASE ${process.env.LOCAL_DB_DATABASE}`)
    .then(() => {
      console.log("Database created.")
    })
}

const createSchema = (client) => {
  // create tables and indices
  const createTableQuery = `
  CREATE TABLE IF NOT EXISTS logs_meta (
    remote_id integer PRIMARY KEY,
    application character varying(255),
    time timestamp without time zone,
    timestamp timestamp without time zone,
    status smallint
  )
  `
  const createTimeIndexQuery = "CREATE INDEX IF NOT EXISTS time_idx ON logs_meta (time)"
  const createTimestampIndexQuery = "CREATE INDEX IF NOT EXISTS timestamp_idx ON logs_meta (timestamp)"
  const createStatusIndexQuery = "CREATE INDEX IF NOT EXISTS status_idx ON logs_meta (status)"
  const createApplicationIndexQuery = "CREATE INDEX IF NOT EXISTS application_idx ON logs_meta (application)"
  return client.query(createTableQuery)
    .then(() => client.query(createTimeIndexQuery))
    .then(() => client.query(createTimestampIndexQuery))
    .then(() => client.query(createStatusIndexQuery))
    .then(() => client.query(createApplicationIndexQuery))
    .then(() => {
      console.log("logs_meta table and indices created (if not exist).")
    })
}


localConnect("postgres")
  .then(postgresClient => checkIfDbExists(postgresClient)
    .then(dbExists => {
      if (dbExists) {
        console.log("Database already created!")
      } else {
        return createDb(postgresClient)
      }
    })
    .then(() => localConnect().then(localClient => localClient))
    .then(localClient => createSchema(localClient))
  )
  .catch(err => {
    console.error(err)
  })
  .finally(() => {
    process.exit()
  })
