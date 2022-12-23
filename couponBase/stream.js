const { Writable, Readable } = require('stream');
const ObjectId = require('mongodb').ObjectId;
const randomString = require('randomstring');
const chalk = require('chalk');
const { connectDb, disconnectDb, getDb } = require('./db');

const getBatchesCollection = () => getDb('bonuzCoupon', 'testeBatches');
const getCouponsCollection = () => getDb('bonuzCoupon', 'testeCoupons');
const getPrizesCollection = () => getDb('bonuz', 'prizes');

const amountCoupons = 1000;

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

    if (this.queue.length % 10_000 === 0) {
      console.log('****** Enqueued chunk, current size: ', this.queue.length);
    }

    if (this.queue.length >= this.queueMaxSize) {
      return this.drainQueue(callback);
    }

    callback(null);
  }

  async drainQueue(callback) {
    let caughtError = null;

    try {
      this.totalOperations += this.queue.length;
      this.emit('queue-draining');
      await this.drainHandler(this.queue);
      this.emit('queue-drained');
    } catch (error) {
      console.log('Caught error on drainQueue', error);
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

  const dateToRemove = new Date();
  dateToRemove.setMonth(dateToRemove.getMonth() + 1);

  await cleanBatches(dateToRemove);
  await cleanCoupons(dateToRemove);

  const expirationDate = new Date();
  expirationDate.setFullYear(expirationDate.getFullYear() + 1);

  const batches = await createBatches(expirationDate);
  await createCoupons(batches, expirationDate);
  await disconnectDb();
  console.timeEnd('Tempo total de processamento');
}

async function cleanBatches(dateToRemove) {
  const query = {
    $or: [
      { expirationDate: { $lte: dateToRemove } },
      { 'coupons.available': { $lte: 0 } }
    ]
  }
  const options = { projection: { _id: 1.0, } };
  const streamExpired = await getBatchesCollection().find(query, options).stream();

  async function deleteBatches(batches) {
    console.log('Executing deleteBatches');
    const objectIds = batches.map((batch) => ObjectId(batch._id));

    const query = {
      _id: { $in: objectIds }
    };

    const result = await getBatchesCollection().deleteMany(query);
    console.log(`deleteBatches finished, batches.length: ${batches.length} | deletedCount: ${result.deletedCount}`);
  }

  const deleteWritable = new QueueWritable(deleteBatches, 1_000);

  streamExpired.pipe(deleteWritable);

  await new Promise((resolve, reject) => {
    streamExpired.on('end', resolve);
    streamExpired.on('error', reject);
  });
}

async function cleanCoupons(dateToRemove) {
  return new Promise(async (resolve, reject) => {
    const query = {
      $or: [
        { expirationDate: { $lte: dateToRemove } },
        { 'status.name': 'expired' }
      ]
    };

    const options = { projection: { _id: 1.0, } };
    const streamExpired = await getCouponsCollection().find(query, options).stream();

    async function deleteCoupons(coupons) {
      console.log('Executing deleteCoupons');
      const objectIds = coupons.map((coupon) => ObjectId(coupon._id));

      const query = {
        _id: { $in: objectIds }
      };

      const result = await getCouponsCollection().deleteMany(query, { writeConcern: { j: false, w: 0 } });
      console.log(`deleteCoupons finished, coupons.length: ${coupons.length} | deletedCount: ${result.deletedCount}`);
    }

    const deleteWritable = new QueueWritable(deleteCoupons, 50_000);

    deleteWritable.on('finish', () => {
      console.log('Finished writing to deleteCoupons');
      resolve();
    });

    streamExpired.on('error', () => {
      streamExpired.destroy();
      deleteWritable.destroy();
      reject();
    });

    streamExpired.pipe(deleteWritable);

    // criar um evento interno para fazer aguardar o drain antes de prosseguir com o pipe

    // await new Promise((resolve, reject) => {
    //   streamExpired.on('end', resolve);
    //   streamExpired.on('error', reject);
    // });
  });
}

async function createBatches(expirationDate) {
  const prizes = await getPrizes();
  const filteredPrizes = await filterEmptyPrizes(prizes);
  const lastBatchCode = await getLastBatchCode();
  const batches = generateBatches(filteredPrizes, lastBatchCode, expirationDate);

  if (batches.length === 0) {
    console.log(chalk.bgYellow.black('Nenhum batch criado! Confira se você possui a collection de prizes configurada corretamente!'));
    throw new Error('No batches created!');
  }

  await getBatchesCollection().insertMany(batches);

  return batches;
}

async function getPrizes() {
  const prizeQuery = {
    'active': true,
    '$or': [{ 'deliveryEngine': 'coupon' }, { 'alliance.name': 'carrefour' }]
  }

  // SELECT prizes.name
  // FROM prizes 
  // WHERE ACTIVE = true AND (delivery = 'coupon' OR allianceName = 'carrefour')
  // LEFT JOIN batches WHERE prize.name = batches.bucket AND (batches.bucket IS NULL OR batches.available = 0)

  const prizeOptions = {
    projection: { _id: 0.0, name: 1.0, 'alliance.name': 1.0, 'alliance.title': 1.0 },
    sort: { _id: -1 }
  }

  const prizeArray = await getPrizesCollection().find(prizeQuery, prizeOptions).toArray();

  return prizeArray;
}

// Filtrar da lista de prizes somente aqueles que não possuem batches
async function filterEmptyPrizes(prizes) {
  const prizeNames = prizes.map((prize) => prize.name);

  const query = { bucket: { $in: prizeNames } };
  const options = { projection: { bucket: 1.0, _id: 0.0 } };

  const batches = await getBatchesCollection().find(query, options).toArray();
  const batchSet = new Set(batches.map((batch) => batch.bucket));

  const emptyPrizes = prizes.filter((prizeName) => !batchSet.has(prizeName));

  return emptyPrizes;
}

async function getLastBatchCode() {
  const options = {
    projection: { code: 1.0, _id: 0 },
    sort: { code: -1.0 }
  }

  const [lastBatch] = await getDb('bonuzCoupon', 'testeBatches').find({}, options).limit(1).toArray();

  if (!lastBatch) {
    return 0;
  }

  return lastBatch.code;
}

function generateBatches(prizes, lastBatchCode, expirationDate) {
  const batches = prizes.map((prize) => createBatch(prize, lastBatchCode, expirationDate));

  return batches;
}

function createBatch(prize, lastBatchCode, expirationDate) {
  const batch = {
    'file': 'loadingCoupons',
    'user': {
      'name': 'Cleverson Rocha',
      'email': 'cleverson.rocha@minu.co'
    },
    'bucket': prize.name,
    'alliance': {
      'name': prize.alliance.name,
      'title': prize.alliance.title,
    },
    'experiences': [

    ],
    'code': lastBatchCode,
    'status': {
      'name': 'processed',
      'timestamp': new Date(),
      'detail': {
        'couponsAffected': amountCoupons,
        'expirationDate': expirationDate
      }
    },
    'trace': [
      {
        'name': 'processed',
        'timestamp': new Date(),
        'detail': {
          'couponsAffected': amountCoupons,
          'expirationDate': expirationDate
        }
      },
      {
        'name': 'processing',
        'timestamp': new Date()
      }
    ],
    'rowsProcessed': amountCoupons,
    'coupons': {
      'available': amountCoupons
    },
    'totalRows': amountCoupons,
    'initialDate': new Date(),
    'expirationDate': expirationDate
  }

  return batch;
}

async function createCoupons(batches, expirationDate) {
  console.log('started createCoupons | batches.length: ', batches.length);
  const createCouponsWritable = new QueueWritable(insertCoupons, 100_000);

  async function insertCoupons(coupons) {
    console.log('Executing insertCoupons');
    await getCouponsCollection().insertMany(coupons);
    console.log(`insertCoupons finished, coupons.length: ${coupons.length}`);
  }

  let promise;
  let resolverRef;

  createCouponsWritable.on('queue-draining', () => {
    console.log('EVENT INTERNAL QUEUE DRAINING RECEIVED');

    if (resolverRef) {
      console.log('removing resolver ref');
      createCouponsWritable.off('queue-drained', resolverRef);
    }

    promise = new Promise((resolve) => {
      resolverRef = () => {
        console.log('DRAINED EVENT RECEIVED');
        resolve();
      };

      createCouponsWritable.on('queue-drained', resolverRef);
    });
  });

  for (const batch of batches) {
    new Array(amountCoupons)
      .fill()
      .map(() =>
        createCouponsWritable.write(generateCoupon(batch, expirationDate))
      );
    // TODO: Pensar em quebrar o numero de cupons gerados por iteração. Caso sejam 20_000 poderia ser um problema

    // createCouponsWritable.write(coupons);
    console.log(`piped ${amountCoupons} coupons to readable`);

    if (promise) {
      console.log('Awaiting coditional promise');
      await promise;
    } else {
      console.log(chalk.bgRed.black('NOT AWAITNG CONDITIONAL PROMISE'));
    }
  }

  createCouponsWritable.end();

  console.log('Will await createCouponsWritable end or error events');
  await new Promise((resolve, reject) => {
    createCouponsWritable.on('close', resolve);
    createCouponsWritable.on('error', reject);
  });
  console.log('Finished createCoupons');
}

function generateCoupon(batch, expirationDate) {
  const couponCode = generateCouponCode();

  const coupon = {
    'alliance': batch.alliance.name,
    'bucket': batch.bucket,
    'coupon': couponCode,
    'expirationDate': expirationDate,
    'initialDateAvaliable': new Date(),
    'created': new Date(),
    'status': {
      'name': 'created',
      'detail': {
        'expirationDate': expirationDate
      },
      'timestamp': new Date()
    },
    'trace': [
      {
        'name': 'created',
        'detail': {
          'expirationDate': expirationDate
        },
        'timestamp': new Date()
      }
    ],
    'batch': {
      'id': ObjectId(batch._id),
      'name': `${batch.name}-${batch.status.timestamp.toJSON()}`,
      'timestamp': batch.status.timestamp
    }
  }

  return coupon;
}

function generateCouponCode() {
  let alphanumeric = randomString.generate({
    length: 8,
    charset: 'alphanumeric'
  });

  prizeCoupon = `MINU${alphanumeric.toUpperCase()}`
};

// async function deleteBatches(batches) {
//   console.log('Executing deleteBatches');
//   const objectIds = batches.map((batch) => ObjectId(batch._id));

//   const query = {
//     _id: { $in: objectIds }
//   };

//   const result = await getBatchesCollection().deleteMany(query);
//   console.log(`deleteBatches finished, batches.length: ${batches.length} | deletedCount: ${result.deletedCount}`);
// }

start();