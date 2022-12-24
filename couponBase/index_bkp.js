const { connectDb, disconnectDb } = require('./db');
const { getDb } = require('./db');
const stream = require('stream');
const ObjectID = require('mongodb').ObjectId;
const randomString = require('randomstring');
const chalk = require('chalk');

const amountCoupons = 100;

const start = async () => {
  try {
    await connectDb();

    //Apaga os documentos expirados, com cupons negativos e zerados na collection batches
    const dbBatchExpiredQuery = {
      $or: [
        {
          expirationDate: {
            $lte: new Date()
          }
        },
        {
          'coupons.available': {
            $lte: 0
          }
        }
      ]
    }
    const dbBatchExpiredOption = {
      projection: {
        _id: 1.0,
        bucket: 1.0,
        'coupons.available': 1.0,
        expirationDate: 1.0
      },
      sort: {
        _id: -1.0
      }
    }

    const dbBatchExpired = await getDb('bonuzCoupon', 'testeBatches').find(dbBatchExpiredQuery, dbBatchExpiredOption).toArray();

    for (const dbBatchExpiredCoupon of dbBatchExpired) {

      let idBatche = dbBatchExpiredCoupon._id

      await getDb('bonuzCoupon', 'testeBatches').deleteOne({ '_id': ObjectID(idBatche) });
    }

    //Apaga os documentos com status expired na collection coupons
    const dbCouponsExpiredQuery = {
      $or: [
        {
          expirationDate: {
            $lte: new Date()
          }
        },
        {
          'status.name': 'expired'
        }
      ]
    }
    const dbCouponsExpiredOption = {
      projection: {
        _id: 1.0,
        expirationDate: 1.0,
        'status.name': 1.0
      },
      sort: {
        _id: -1.0
      }
    }

    const dbCouponsExpired = await getDb('bonuzCoupon', 'testeCoupons').find(dbCouponsExpiredQuery, dbCouponsExpiredOption).toArray();

    for (const dbCouponsExpiredCoupon of dbCouponsExpired) {

      let idPrize = dbCouponsExpiredCoupon._id

      await getDb('bonuzCoupon', 'testeCoupons').deleteOne({ '_id': ObjectID(idPrize) });
    }


    //Definição da data de expiração dos cupons (validade de um ano)
    const expirationDate = new Date();
    expirationDate.setFullYear(expirationDate.getFullYear() + 1);


    //busca por prizes ativos, cupons e Carrefour
    const prizeQuery = {
      'active': true,
      '$or': [
        {
          'deliveryEngine': 'coupon'
        },
        {
          'alliance.name': 'carrefour'
        }
      ]
    }
    const prizeOptions = {
      projection: {
        _id: 0.0,
        name: 1.0,
        'alliance.name': 1.0,
        'alliance.title': 1.0
      },
      sort: {
        _id: -1
      }
    }

    const prizeArray = await getDb('bonuz', 'prizes').find(prizeQuery, prizeOptions).toArray();


    //Busca na collection batches pelo sequencial
    const codeQuery = {}
    const codeOptions = {
      projection: {
        code: 1.0,
        _id: 0,
      },
      sort: {
        code: -1.0
      }
    }

    const codeSearch = await getDb('bonuzCoupon', 'testeBatches').find(codeQuery, codeOptions).limit(1).toArray();


    if (!codeSearch.length) {
      resultCode = 0
    } else {
      resultCode = codeSearch[0].code
    }

    //Iteração para a criação do arquivo batche na collection batches
    for (const prizeList of prizeArray) {

      //sequencial
      resultCode++

      await getDb('bonuzCoupon', 'testeBatches').insertMany([
        {
          'file': 'loadingCoupons',
          'user': {
            'name': 'Cleverson Rocha',
            'email': 'cleverson.rocha@minu.co'
          },
          'bucket': prizeList.name,
          'alliance': {
            'name': prizeList.alliance.name,
            'title': prizeList.alliance.title,
          },
          'experiences': [

          ],
          'code': resultCode,
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

      ]);


      //Consulta batches para popular o campo batch na collection coupons
      const query = {
        'bucket': prizeList.name
      }
      const options = {
        projection: {
          _id: 1.0,
          bucket: 1.0,
          'status.timestamp': 1.0
        },
        sort: {
          _id: -1
        }
      }

      const bucketProperties = await getDb('bonuzCoupon', 'testeBatches').findOne(query, options);

      const dbBatcheTimestemp = bucketProperties.status.timestamp;
      const dbBatcheId = bucketProperties._id;
      const jsonDate = dbBatcheTimestemp.toJSON(); //converte o timestamp para concatenar com o name no campo batch na collection coupons

      for (i = 0; i < amountCoupons; i++) {

        //gerador de cupons randômico
        const codeGenerator = () => {
          let alphanumeric = randomString.generate({
            length: 8,
            charset: 'alphanumeric'
          });

          prizeCoupon = `MINU${alphanumeric.toUpperCase()}`
        };

        codeGenerator()

        await getDb('bonuzCoupon', 'testeCoupons').insertMany([
          {
            'alliance': prizeList.alliance.name,
            'bucket': prizeList.name,
            'coupon': prizeCoupon,
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
              'id': ObjectID(dbBatcheId),
              'name': `${prizeList.name}-${jsonDate}`,
              'timestamp': dbBatcheTimestemp
            }
          }
        ]);
      };
    }


    //Total geral de cupons
    const totalAmountCoupons = amountCoupons * prizeArray.length;
    console.log(`Total de prizes inseridos: ${chalk.blue.bold(prizeArray.length)}!`);
    console.log(`Collection coupons => Foram inseridos ${chalk.green(amountCoupons)} cupons para cada prize!`);
    console.log(`Total de cupons inseridos na base: ${chalk.yellowBright(totalAmountCoupons)}!`);


    //Total de documentos apagados
    const dbDeleteBatches = dbBatchExpired.length
    console.log(`Collection batches => Foram apagados um total de ${chalk.red(dbDeleteBatches)} documentos expirados ou com base zerada!`);

    const dbDeleteCouponsExpired = dbCouponsExpired.length
    console.log(`Collection coupons => Foram apagados um total de ${chalk.red(dbDeleteCouponsExpired)} documentos com status Expired!`);

  } catch (err) {
    console.error(err);
  } finally {
    await disconnectDb()
  };
};

start();