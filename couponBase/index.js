const fs = require('fs');
const { connectDb, disconnectDb } = require('./db');
const { getDb } = require('./db');

const start = async () => {
  try {
    await connectDb();

    //busca por prizes ativos, cupons e Carrefour
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
    const allianceNameArray = arrayMany.map((prize) => prize.alliance.name);
    const allianceTitleArray = arrayMany.map((prize) => prize.alliance.title);

    //Busca na collection batches pelo sequencial
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

    const dbCode = codeSearch.map((testeBatches) => testeBatches.code)

    let resultCode = dbCode[0]

    if (!resultCode) {
      resultCode = 0
    }

    //Definição da data de expriração dos cupons (validade de um ano)
    const expirationDate = new Date();
    expirationDate.setFullYear(expirationDate.getFullYear() + 1);

    //Iteração para a criação do arquivo batche na collection batches
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
            "name": 'allianceNameArray',
            "title": 'allianceTitleArray'
          },
          "code": resultCode, //sequencial do documento inserido na collection
          "status": {
            "name": "processed",
            "timestamp": new Date(), //Data de inserção na collection?
            "detail": {
              "couponsAffected": 200,
              "expirationDate": expirationDate //Data de expiração dos cupons
            }
          },
          "trace": [
            {
              "name": "processed",
              "timestamp": new Date(), //Data de inserção na collection?
              "detail": {
                "couponsAffected": 200,
                "expirationDate": expirationDate //Data de expiração dos cupons
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
          "expirationDate": expirationDate //Data de expiração dos cupons
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