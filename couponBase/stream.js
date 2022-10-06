const { connectDb, disconnectDb } = require('./db');
const { getDb } = require('./db');
const stream = require('stream');
const ObjectID = require('mongodb').ObjectId;
const randomString = require('randomstring');
const chalk = require('chalk');

const amountCoupons = 100;

async function start() {
  await connectDb();
  await cleanBatches();
  await cleanCoupons();
  const buckets = await createBatches();
  await createCoupons(buckets);
}

async function cleanBatches() {
  const dbBatchExpiredQuery = {
    $or: [
      {
        expirationDate: {
          $lte: new Date()
        }
      },
      {
        'coupons.available': {
          $lte: 0
        }
      }
    ]
  }

  const dbBatchExpiredOption = {
    projection: {
      _id: 1.0,
      bucket: 1.0,
      'coupons.available': 1.0,
      expirationDate: 1.0
    },
    sort: {
      _id: -1.0
    }
  }

  const streamExpired = await getDb('bonuzCoupon', 'testeBatches')
    .find(dbBatchExpiredQuery, dbBatchExpiredOption)
    .stream();


}