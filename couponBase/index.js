const { connectDb, disconnectDb } = require('./db');
const { getDb } = require('./db');

const start = async () => {
  try {
    await connectDb();

    const arrayMany = await getDb('bonuz', 'prizes').find(
      {
        "active": true,
        "deliveryEngine": "coupon"
      }).project(
        {
          "name": 1.0,
          "_id": 0
        }
      ).toArray()

    const prizeArray = arrayMany.map((prize) => prize.name)
    console.log(prizeArray)

    // for...of

  } catch (err) {
    console.error(err);
  } finally {
    await disconnectDb()
  };
}

start();

// db = db.getSiblingDB("bonuz");
// db.getCollection("prizes").find(
//   {
//     "active": true,
//     "deliveryEngine": "coupon"
//   },
//   {
//     "name": 1.0
//   }
// );














// db = db.getSiblingDB("bonuzCoupon");
// db.getCollection("coupons").find(
//   {
//     "bucket": {
//       "$in": [
//         "carrefour-eco-tupper-500ml",
//         "carrefour-eco-tupper-1l",
//         "carrefour-tupperfresh-quadrado-200ml-2-un",
//         "carrefour-tupperfresh-quadrado-1l",
//         "carrefour-tupperfresh-retangular-1-6l",
//         "carrefour-cj-super-instantaneas-slim-1-3l-2-25l-2-un",
//         "carrefour-cj-tigelas-maravilhosas-500ml-1l-1-8l-3-un",
//         "carrefour-eco-tupper-1l-moeda-dinheiro",
//         "carrefour-tupperfresh-quadrado-200ml-2-un-moeda-dinheiro",
//         "carrefour-tupperfresh-quadrado-1l-moeda-dinheiro",
//         "carrefour-tupperfresh-retangular-1-6l-moeda-dinheiro",
//         "carrefour-cj-super-instantaneas-slim-1-3l-2-25l-2-un-moeda-dinheiro",
//         "carrefour-cj-tigelas-maravilhosas-500ml-1l-1-8l-3-un-moeda-dinheiro",
//         "carrefour-turbo-chef-300ml-moeda-dinheiro"
//       ]
//     }
//   }
// ).sort(
//   {
//     "_id": -1.0
//   }
// );