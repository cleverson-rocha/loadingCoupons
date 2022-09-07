const cliProgress = require('cli-progress');
const chalk = require('chalk');
const randomString = require('randomstring');
const { connectDb, disconnectDb } = require('./db');
const { getDb } = require('./db');
const fs = require('fs');

const start = async () => {
  try {
    await connectDb();

    const arrayMany = await getDb('bonuz', 'prizes').find(
      {
        "active": true,
        "$or": [
          {
            "deliveryEngine": "coupon"
          },
          {
            "alliance.name": "carrefour"
          }
        ]
      },
      {
        "name": 1.0,
        "_id": 0.0
      }
    ).toArray()

    const prizeArray = arrayMany.map((prize) => prize.name)
    // console.log(prizeArray)

    fs.writeFile('./logs/listaPrizes' + '.txt', prizeArray.join('\n'), (err, data) => {
      if (err) {
        return
      }
    });

    //Gerador de código
    function createCodeAlphanumeric() {
      let alphanumeric = randomString.generate({
        length: 12,
        charset: 'alphanumeric'
      });

      stringCode = alphanumeric.toUpperCase()
    };

    createCodeAlphanumeric()

    // for...of

    for (const prizeList of prizeArray) {

      await getDb('bonuzCoupon', 'testeCoupons').insertMany([
        {
          "alliance": "teste_01",
          "bucket": "carrefour-agenda-ou-planner",
          "coupon": stringCode,
          "expirationDate": "2022-12-31T12:00:00.000+0000",
          "initialDateAvaliable": "2022-09-07T12:00:00.000+0000",
          "created": "2022-09-07T12:00:00.000+0000",
          "status": {
            "name": "issued",
            "timestamp": "2021-06-29T13:37:10.916+0000",
            "detail": {
              "expirationDate": "2021-12-31T03:00:00.000Z"
            }
          },
          "trace": [
            {
              "name": "issued",
              "timestamp": "2021-06-29T13:37:10.916+0000",
              "detail": {
                "expirationDate": "2021-12-31T03:00:00.000Z"
              }
            },
            {
              "name": "created",
              "detail": {
                "expirationDate": "2022-12-31T12:00:00.000+0000"
              },
              "timestamp": "2022-09-07T12:00:00.000+0000"
            }
          ],
          "batch": {
            "name": "teste-teste-01-2022-09-07T12:00:00.500Z",
            "timestamp": "2022-09-07T12:00:00.000+0000"
          }
        }
      ]);
      // console.log(prizeList);

      // Progress bar
      const bar1 = new cliProgress.SingleBar({}, cliProgress.Presets.legacy);

      bar1.start(prizeList.length, 0);
      bar1.update(100);
      bar1.stop();
    };
  } catch (err) {
    console.error(err);
  } finally {
    await disconnectDb()
  };
}

start();