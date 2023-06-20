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

    const expirationDate = new Date();
    expirationDate.setFullYear(expirationDate.getFullYear() + 1);
    const name = 'luckau';
    const title = 'Luckau';
    const prizeArray = [
      {
        alliance: { name: name, title: title },
        name: 'luckau-desconto-r12'
      }
    ];

    //Busca na collection batches pelo sequencial
    const codeQuery = {};
    const codeOptions = {
      projection: {
        code: 1.0,
        _id: 0
      },
      sort: {
        code: -1.0
      }
    };

    const codeSearch = await getDb('bonuzCoupon', 'batches').find(codeQuery, codeOptions).limit(1).toArray();

    if (!codeSearch.length) {
      resultCode = 0;
    } else {
      resultCode = codeSearch[0].code;
    }

    //Iteração para a criação do arquivo batche na collection batches
    for (const prizeList of prizeArray) {
      //sequencial
      resultCode++;

      await getDb('bonuzCoupon', 'batches').insertMany([
        {
          'file': 'loadingCoupons',
          'user': {
            'name': 'Cleverson Rocha',
            'email': 'cleverson.rocha@minu.co'
          },
          'bucket': prizeList.name,
          'alliance': {
            'name': prizeList.alliance.name,
            'title': prizeList.alliance.title
          },
          'experiences': [],
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
      };
      const options = {
        projection: {
          _id: 1.0,
          bucket: 1.0,
          'status.timestamp': 1.0
        },
        sort: {
          _id: -1
        }
      };

      const bucketProperties = await getDb('bonuzCoupon', 'batches').findOne(query, options);

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

          prizeCoupon = `MINU${alphanumeric.toUpperCase()}`;
        };

        codeGenerator();

        await getDb('bonuzCoupon', 'coupons').insertMany([
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
      }
    }

    const totalAmountCoupons = amountCoupons * prizeArray.length;
    console.log(`Total de prizes inseridos: ${chalk.blue.bold(prizeArray.length)}!`);
    console.log(`Collection coupons => Foram inseridos ${chalk.green(amountCoupons)} cupons para cada prize!`);
    console.log(`Total de cupons inseridos na base: ${chalk.yellowBright(totalAmountCoupons)}!`);
  } catch (err) {
    console.error(err);
  } finally {
    await disconnectDb();
  }
};

start();
