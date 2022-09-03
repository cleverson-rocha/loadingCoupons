const chalk = require('chalk');
const { MongoClient } = require('mongodb');

const user = 'ex3_admin'
const password = 'frota123456'

// const uri = `mongodb+srv://${user}:${password}@cluster0.vaa3i.mongodb.net/?retryWrites=true&w=majority`
const uri = 'mongodb://localhost:27017/'

const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });

const connectDb = async () => {
  try {
    await client.connect();
    console.log(chalk.bgGreen('Connected to DB!'))
  } catch (error) {
    console.log(chalk.bgRedBright('Error connecting to DB, Detail: ', error));
  }
};

const disconnectDb = async () => {
  try {
    await client.close();
    console.log(chalk.bgHex('#FFA500')('Database close successfully!'))
  } catch (error) {
    console.log(chalk.bgRedBright('Error closing DB, Detail: ', error));
  }
};

const getDb = () => {
  const dbName = process.env.DB_NAME || 'bonuzCoupon';
  return client.db(dbName);
}

const toObjectId = (string) => {
  return ObjectId(stringId);
}

module.exports = {
  connectDb,
  disconnectDb,
  getDb,
  toObjectId
};