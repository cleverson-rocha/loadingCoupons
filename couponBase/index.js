const cliProgress = require('cli-progress');
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

    // for...of
    let i = 1;
    while (i < 2000) {

      for (const prizeList of prizeArray) {

        // Gerador de código
        const alphanumeric = randomString.generate({
          length: 12,
          charset: 'alphanumeric'
        });
        const stringCode = alphanumeric.toUpperCase()

        await getDb('bonuzCoupon', 'testeCoupons').insertMany([
          {
            "alliance": "teste_01",
            "bucket": prizeList,
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
      };
      // Incremento da variável de controle While
      i++;

      // // Calculo de iteração
      // const totalProgress = i * prizeArray.length

      // // create new progress bar
      // const barProgress = new cliProgress.SingleBar({}, cliProgress.Presets.legacy);
      // barProgress.start(totalProgress, 0);
      // barProgress.update(stringCode);
      // barProgress.stop();
    };

  } catch (err) {
    console.error(err);
  } finally {
    await disconnectDb()
  };
};

start();