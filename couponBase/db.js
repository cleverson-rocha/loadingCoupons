const chalk = require('chalk');
const { MongoClient } = require('mongodb');

const user = 'xxxxxxxxxxxx'
const password = 'xxxxxxxxxxxxxx'

// const uri = `mongodb+srv://${user}:${password}@xewards.xat5y.mongodb.net/test`
const uri = 'mongodb://localhost:27017/'

const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });

const connectDb = async () => {
  try {
    await client.connect();
    console.log(chalk.bgGreen.black('Connected to DB!'))
  } catch (error) {
    console.log(chalk.bgRedBright.black('Error connecting to DB, Detail: ', error));
  }
};

const disconnectDb = async () => {
  try {
    await client.close();
    console.log(chalk.bgGreen.black('Database close successfully!'))
  } catch (error) {
    console.log(chalk.bgRedBright.black('Error closing DB, Detail: ', error));
  }
};

const getDb = (dbName, collectionName) => {
  return client.db(dbName).collection(collectionName)
}

module.exports = {
  connectDb,
  disconnectDb,
  getDb,
};