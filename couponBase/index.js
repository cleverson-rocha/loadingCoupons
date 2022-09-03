const { connectDb, disconnectDb } = require('./db');
const { getDb } = require('./db');

const start = async () => {
  try {
    await connectDb();

  } catch (err) {
    console.error(err);
  };
}

start();

const banco = getDb.collection("coupons")

const prizeList = async () => {
  return await banco().find({
    "bucket": {
      "$in": [
        "carrefour-agenda-ou-planner",
        "carrefour-mochila-infantil"
      ]
    }
  })
};
console.log(prizeList)