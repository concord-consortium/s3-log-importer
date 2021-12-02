const { Client } = require('pg')

require('dotenv').config()

exports.localConnect = (database) => {
  database = database || process.env.LOCAL_DB_DATABASE

  const client = new Client({
    user: process.env.LOCAL_DB_USER,
    host: process.env.LOCAL_DB_HOST,
    database,
    password: process.env.LOCAL_DB_PASSWORD,
    port: process.env.LOCAL_DB_PORT,
  })

  return client.connect().then(() => client)
}

exports.remoteConnect = () => {
  const client = new Client({
    user: process.env.REMOTE_DB_USER,
    host: process.env.REMOTE_DB_HOST,
    database: process.env.REMOTE_DB_DATABASE,
    password: process.env.REMOTE_DB_PASSWORD,
    port: process.env.REMOTE_DB_PORT,
  })

  return client.connect().then(() => client)
}
