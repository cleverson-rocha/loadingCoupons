const fs = require('fs');
const { connectDb, disconnectDb } = require('./db');
const { getDb } = require('./db');

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
        "alliance.name": 1.0,
        "alliance.title": 1.0,
        "_id": 0.0
      }
    ).toArray()

    const prizeArray = arrayMany.map((prize) => prize.name);
    // const allianceNameArray = arrayMany.map((prize) => prize.alliance.name);
    // const allianceTitleArray = arrayMany.map((prize) => prize.alliance.title);

    //insert na batch

    codeSearch = await getDb('bonuzCoupon', 'testeBatches').find({},
      {
        "code": 1.0,
        "_id": 0.0
      }
    ).sort(
      {
        "code": -1.0
      }
    ).limit(1).toArray();

    //   codeSearch = [{code: 123}] 
    let dbCode = codeSearch.map((testeBatches) => testeBatches.code)
    //code = [123]

    let resultCode = dbCode[0]
    //resultcode = 123

    if (!resultCode) {
      resultCode = 0
    }

    for (const prizeList of prizeArray) {

      resultCode++

      await getDb('bonuzCoupon', 'testeBatches').insertMany([
        {
          "file": "loadingCoupons.txt", //nome do arquivo txt utilizado para carregar cupons na base
          "user": { //Usuário que inseriu a base de cupons
            "name": "Cleverson Rocha",
            "email": "cleverson.rocha@minu.co"
          },
          "bucket": prizeList,
          "alliance": {
            "name": "allianceNameArray",
            "title": "allianceTitleArray"
          },
          "code": resultCode, //sequencial dos documentos na collection
          "status": {
            "name": "processed",
            "timestamp": new Date(), //Data de inserção na collection?
            "detail": {
              "couponsAffected": 200,
              "expirationDate": "2023-12-31T03:00:00.000+0000" //Data de expiração dos cupons
            }
          },
          "trace": [
            {
              "name": "processed",
              "timestamp": new Date(), //Data de inserção na collection?
              "detail": {
                "couponsAffected": 200,
                "expirationDate": "2023-12-31T03:00:00.000+0000" //Data de expiração dos cupons
              }
            },
            {
              "name": "processing",
              "timestamp": new Date() //Data de inserção na collection?
            }
          ],
          "rowsProcessed": 200, //cupons carregados na base
          "coupons": {
            "available": 174 //cupons disponíveis
          },
          "totalRows": 200, //Total de linhas no documento utilizado para carregar os cupons
          "initialDate": new Date(), //Data de inserção na collection?
          "expirationDate": "2023-12-31T03:00:00.000+0000" //Data de expiração dos cupons
        }
      ]);
    };

    // fs.writeFile('./logs/listaPrizes' + '.txt', prizeArray.join('\n'), (err, data) => {
    //   if (err) {
    //     return
    //   }
    // });
    // fs.writeFile('./logs/allianceName' + '.txt', allianceNameArray.join('\n'), (err, data) => {
    //   if (err) {
    //     return
    //   }
    // });
    // fs.writeFile('./logs/allianceTitle' + '.txt', allianceTitleArray.join('\n'), (err, data) => {
    //   if (err) {
    //     return
    //   }
    // });

  } catch (err) {
    console.error(err);
  } finally {
    await disconnectDb()
  };
};

start();