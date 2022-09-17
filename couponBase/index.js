const { connectDb, disconnectDb } = require('./db');
const { getDb } = require('./db');
const ObjectID = require('mongodb').ObjectId;
const randomString = require('randomstring');

const amountCoupons = 10;

const start = async () => {
  try {
    await connectDb();

    //Apaga todos os documentos das collections
    await getDb('bonuzCoupon', 'testeCoupons').deleteMany({});
    await getDb('bonuzCoupon', 'testeBatches').deleteMany({});

    //Definição da data de expiração dos cupons (validade de um ano)
    const expirationDate = new Date();
    expirationDate.setFullYear(expirationDate.getFullYear() + 1);

    //busca por prizes ativos, cupons e Carrefour
    const query = {
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
    const options = {
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

    const prizeArray = await getDb('bonuz', 'prizes').find(query, options).toArray();

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

    //Total de iterações para alimentar a collection batches.coupons.available
    const totalAmountCoupons = amountCoupons * prizeArray.length
    console.log(totalAmountCoupons)

    //Iteração para a criação do arquivo batche na collection batches
    for (const prizeList of prizeArray) {

      //sequencial
      resultCode++

      await getDb('bonuzCoupon', 'testeBatches').insertMany([
        {
          'file': 'loadingCoupons', //meio pelo qual carregamos a base
          'user': { //Usuário que inseriu a base de cupons
            'name': 'Cleverson Rocha',
            'email': 'cleverson.rocha@minu.co'
          },
          'bucket': prizeList.name,
          'alliance': {
            'name': prizeList.alliance.name,
            'title': prizeList.alliance.title,
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
            'available': totalAmountCoupons //cupons disponíveis
          },
          'totalRows': 200, //Total de linhas no documento utilizado para carregar os cupons
          'initialDate': new Date(), //Data de inserção na collection?
          'expirationDate': expirationDate //Data de expiração dos cupons
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

      for (i = 0; i <= amountCoupons; i++) {

        //gerador de cupons randômico
        const codeGenerator = () => {
          let alphanumeric = randomString.generate({
            length: 19,
            charset: 'alphanumeric'
          });

          prizeCoupon = alphanumeric
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
              'id': ObjectID(dbBatcheId),
              'name': `${prizeList.name}-${jsonDate}`,
              'timestamp': dbBatcheTimestemp
            }
          }
        ]);
      };
    }

  } catch (err) {
    console.error(err);
  } finally {
    await disconnectDb()
  };
};

start();