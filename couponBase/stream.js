const { Writable } = require('stream');
const ObjectID = require('mongodb').ObjectId;
const randomString = require('randomstring');
const chalk = require('chalk');
const { connectDb, disconnectDb, getDb } = require('./db');

const amountCoupons = 100;

class QueueWritable extends Writable {
  constructor(drainHandler, queueMaxSize, options) {
    super({ ...options, objectMode: true });
    this.queue = [];
    this.queueMaxSize = queueMaxSize;
    this.drainHandler = drainHandler;
  }

  _write(chunk, encoding, callback) {
    console.log('chamou o _write');

    this.queue.push(chunk);
    console.log('chunk enqueued, lenght', this.queue.length);

    if (this.queue.length === this.queueMaxSize) {
      return this.drainQueue(callback);
    }

    callback(null);
  }

  async drainQueue(callback) {
    let caughtError = null;

    try {
      console.log('draining queue');
      await this.drainHandler(this.queue);
    } catch (error) {
      caughtError = error;
    } finally {
      this.queue = [];
      callback(caughtError);
    }
  }

  _final(callback) {
    console.log('chamou o final, length', this.queue.length);

    if (this.queue.length > 0) {
      this.drainQueue(callback)
    } else {
      callback(null);
    }

    console.log('finished');
  }
}

async function start() {
  await connectDb();
  await cleanBatches();
  // await cleanCoupons();
  // const buckets = await createBatches();
  // await createCoupons(buckets);
  await disconnectDb();
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

  const deleteWritable = new QueueWritable(deleteBatches, 500);

  streamExpired.pipe(deleteWritable);

  await new Promise(function (resolve, reject) {
    streamExpired.on('end', resolve);
    streamExpired.on('error', reject); // or something like that. might need to close `hash`
  });
}

async function deleteBatches(batches) {
  console.log('deleteBatches called', batches);
}

start();