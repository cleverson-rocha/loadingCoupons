const { connectDb, disconnectDb } = require('./db');
const { getDb } = require('./db');

const start = async () => {
  try {
    await connectDb();
    const arr = await getDb('bonuz', 'experiences').find({ name: 'carrefour-meu-game-carrefour' }).toArray()
    console.log(arr[0].prizes.length)

  } catch (err) {
    console.error(err);
  };
}

start();




// delivery engine coupon
// if carrefour