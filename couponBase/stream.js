const { Writable } = require('stream');
const ObjectId = require('mongodb').ObjectId;
const randomString = require('randomstring');
const chalk = require('chalk');
const { connectDb, disconnectDb, getDb } = require('./db');

const getBatchesCollection = () => getDb('bonuzCoupon', 'testeBatches');
const getCouponsCollection = () => getDb('bonuzCoupon', 'testeCoupons');

const amountCoupons = 100;

class QueueWritable extends Writable {
  constructor(drainHandler, queueMaxSize, options) {
    super({ ...options, objectMode: true });
    this.totalOperations = 0;
    this.queue = [];
    this.queueMaxSize = queueMaxSize;
    this.drainHandler = drainHandler;
  }

  _write(chunk, encoding, callback) {
    this.queue.push(chunk);

    if (this.queue.length === this.queueMaxSize) {
      return this.drainQueue(callback);
    }

    callback(null);
  }

  async drainQueue(callback) {
    let caughtError = null;

    try {
      this.totalOperations += this.queue.length;
      await this.drainHandler(this.queue);
    } catch (error) {
      caughtError = error;
    } finally {
      this.queue = [];
      callback(caughtError);
    }
  }

  _final(callback) {
    if (this.queue.length > 0) {
      this.drainQueue(callback)
    } else {
      callback(null);
    }

    console.log('finished, totalOperations:', this.totalOperations);
  }
}

async function start() {
  console.time('Tempo total de processamento');
  await connectDb();
  await cleanBatches();
  await cleanCoupons();
  // const buckets = await createBatches();
  // await createCoupons(buckets);
  await disconnectDb();
  console.timeEnd('Tempo total de processamento');
}

async function cleanBatches() {
  const query = {
    $or: [
      { expirationDate: { $lte: new Date() } },
      { 'coupons.available': { $lte: 0 } }
    ]
  }
  const options = { projection: { _id: 1.0, } };
  const streamExpired = await getBatchesCollection().find(query, options).stream();

  const deleteWritable = new QueueWritable(deleteBatches, 1_000);

  streamExpired.pipe(deleteWritable);

  await new Promise((resolve, reject) => {
    streamExpired.on('end', resolve);
    streamExpired.on('error', reject);
  });
}

async function deleteBatches(batches) {
  console.log('Executing deleteBatches');
  const objectIds = batches.map((batch) => ObjectId(batch._id));

  const query = {
    _id: { $in: objectIds }
  };

  const result = await getBatchesCollection().deleteMany(query);
  console.log(`deleteBatches finished, batches.length: ${batches.length} | deletedCount: ${result.deletedCount}`);
}

async function cleanCoupons() {
  const query = {
    $or: [
      { expirationDate: { $lte: new Date() } },
      { 'status.name': 'expired' }
    ]
  };

  const options = { projection: { _id: 1.0, } };
  const streamExpired = await getCouponsCollection().find(query, options).stream();

  const deleteWritable = new QueueWritable(deleteCoupons, 50_000);

  streamExpired.pipe(deleteWritable);

  await new Promise((resolve, reject) => {
    streamExpired.on('end', resolve);
    streamExpired.on('error', reject);
  });
}

async function deleteCoupons(coupons) {
  console.log('Executing deleteCoupons');
  const objectIds = coupons.map((coupon) => ObjectId(coupon._id));

  const query = {
    _id: { $in: objectIds }
  };

  const result = await getCouponsCollection().deleteMany(query, { writeConcern: { j: false, w: 0 } });
  console.log(`deleteCoupons finished, coupons.length: ${coupons.length} | deletedCount: ${result.deletedCount}`);
}

start();