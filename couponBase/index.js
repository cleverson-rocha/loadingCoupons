const fs = require('fs');
const { connectDb, disconnectDb } = require('./db');
const { getDb } = require('./db');
const { codeGenerator } = require('./randomString')
const ObjectID = require('mongodb').ObjectId;

//Construtor de data string
const event = new Date();
const jsonDate = event.toJSON();

const start = async () => {
  try {
    await connectDb();

    // await getDb('bonuzCoupon', 'testeCoupons').deleteMany({});
    // await getDb('bonuzCoupon', 'testeBatches').deleteMany({});

    //busca por prizes ativos, cupons e Carrefour
    const arrayMany = await getDb('bonuz', 'prizes').find(
      {
        'active': true,
        '$or': [
          {
            'deliveryEngine': 'coupon'
          },
          {
            'alliance.name': 'carrefour'
          }
        ]
      },
      {
        'name': 1.0,
        'alliance.name': 1.0,
        'alliance.title': 1.0,
        '_id': 0.0
      }
    ).toArray()

    //corrigir projeção

    const prizeArray = arrayMany.map((prize) => prize.name);
    const allianceNameArray = arrayMany.map((prize) => prize.alliance.name);
    const allianceTitleArray = arrayMany.map((prize) => prize.alliance.title);

    //Busca na collection batches pelo sequencial
    codeSearch = await getDb('bonuzCoupon', 'testeBatches').find({},
      {
        'code': 1.0,
        '_id': 0.0
      }
    ).sort(
      {
        'code': -1.0
      }
    ).limit(1).toArray();

    const dbCode = codeSearch.map((testeBatches) => testeBatches.code)

    let resultCode = dbCode[0]

    if (!resultCode) {
      resultCode = 0
    }

    //Definição da data de expiração dos cupons (validade de um ano)
    const expirationDate = new Date();
    expirationDate.setFullYear(expirationDate.getFullYear() + 1);

    //Iteração para a criação do arquivo batche na collection batches
    for (const prizeList of prizeArray) {

      resultCode++
      // prizeCoupon = codeGenerator()
      // console.log(prizeCoupon)

      await getDb('bonuzCoupon', 'testeBatches').insertMany([
        {
          'file': 'loadingCoupons', //meio pelo qual carregamos a base
          'user': { //Usuário que inseriu a base de cupons
            'name': 'Cleverson Rocha',
            'email': 'cleverson.rocha@minu.co'
          },
          'bucket': prizeList,
          'alliance': {
            'name': 'allianceNameList',
            'title': 'allianceTitleList'
          },
          'code': resultCode, //sequencial do documento inserido na collection
          'status': {
            'name': 'processed',
            'timestamp': new Date(), //Data de inserção na collection?
            'detail': {
              'couponsAffected': 200,
              'expirationDate': expirationDate //Data de expiração dos cupons
            }
          },
          'trace': [
            {
              'name': 'processed',
              'timestamp': new Date(), //Data de inserção na collection?
              'detail': {
                'couponsAffected': 200,
                'expirationDate': expirationDate //Data de expiração dos cupons
              }
            },
            {
              'name': 'processing',
              'timestamp': new Date() //Data de inserção na collection?
            }
          ],
          'rowsProcessed': 200, //cupons carregados na base
          'coupons': {
            'available': 174 //cupons disponíveis
          },
          'totalRows': 200, //Total de linhas no documento utilizado para carregar os cupons
          'initialDate': new Date(), //Data de inserção na collection?
          'expirationDate': expirationDate //Data de expiração dos cupons
        }
      ]);

      // const bucketProperties = await getDb('bonuzCoupon', 'testeBatches').findOne(
      //   {
      //     'bucket': prizeList
      //   },
      //   {
      //     projection: {
      //       '_id': 1,
      //       'status.timestamp': 1,
      //       'bucket': 1
      //     },
      //   },
      //   {
      //     sort: {
      //       '_id': -1
      //     }
      //   },
      //   function (err, result) {
      //     console.log(err);
      //   }
      // )

      const query = { "bucket": prizeList }
      const options = {
        projection: {
          _id: 1,
          'status.timestamp': 1,
          'bucket': 1
        },
        sort: {
          _id: -1
        }
      }

      const bucketProperties = await getDb('bonuzCoupon', 'testeBatches').findOne(query, options);

      const batcheTimestemp = bucketProperties.status.timestamp;
      const jsonDate = batcheTimestemp.toJSON();


      // console.log('bucketProperties;', bucketProperties)

      await getDb('bonuzCoupon', 'testeCoupons').insertMany([
        {
          'alliance': 'allianceNameList',
          'bucket': prizeList,
          'coupon': 'prizeCoupon',
          'expirationDate': expirationDate,
          'initialDateAvaliable': new Date(),
          'created': new Date(),
          'status': {
            'name': 'expired',
            'timestamp': new Date()
          },
          'trace': [
            {
              'name': 'expired',
              'timestamp': new Date()
            },
            {
              'name': 'created',
              'detail': {
                'expirationDate': expirationDate
              },
              'timestamp': new Date()
            }
          ],
          'batch': {
            'id': ObjectID(bucketProperties._id),
            'name': `${prizeList}-${jsonDate}`,
            'timestamp': bucketProperties.status.timestamp
          }
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