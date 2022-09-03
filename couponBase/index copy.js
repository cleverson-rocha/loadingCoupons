const { MongoClient } = require('mongodb');
const mongodbConnect = require('mongodbConnect');

//Conectar ao mongodb
async function main() {
  const uri = 'mongodb://localhost:27017/';
  // const uri = 'mongodb+srv://ex3_admin:frota123456@cluster0.vaa3i.mongodb.net/?retryWrites=true&w=majority'

  const client = new MongoClient(uri);

  try {
    await client.connect();

    //Aguarda a lista de dbs disponíveis
    // await listDatabases(client);

    //Chamar função

    //createOne
    // await createListing(client, {
    //   name: 'Lovely loft',
    //   summary: 'A charming loft in Paris',
    //   bedrooms: 1,
    //   bathrooms: 1
    // });

    //createMany
    // await createMultipleListings(client, [
    //   {
    //     name: 'Beautiful Beach House',
    //     summary: 'Enjoy relaxed beach living in this house with a private beach',
    //     bedrooms: 4,
    //     bathrooms: 2.5,
    //     beds: 7,
    //     last_review: new Date()
    //   },
    //   {
    //     name: 'Private room in London',
    //     summary: 'Apartment',
    //     bedrooms: 1,
    //     bathrooms: 1
    //   },
    //   {
    //     name: 'Infinite Views',
    //     summary: 'Modern home with infinite views from the infinity pool',
    //     property_type: 'House',
    //     bedrooms: 5,
    //     bathrooms: 4.5,
    //     beds: 5
    //   },
    // ]);

    //findOne
    // await findOneListingByName(client, 'Infinite Views');

    //findMany
    // await findListingsWithMinimumBedroomsBathroomsAndMostRecentReviews(client, {
    //   minimumNumberOfBedrooms: 4,
    //   minimumNumberOfBathrooms: 2,
    //   maximumNumberOfResults: 5
    // });

    //updateOne
    // await updateListingByName(client, 'Infinite Views', {
    //   bedrooms: 6,
    //   beds: 8
    // });

    //upsert
    // await upsertListingByName(client, 'Cozy Cottage', {
    //   bedrooms: 2,
    //   bathrooms: 2
    // });

    //updateMany
    // await updateAllListingsToHavePropertyType(client);


    //deleteOne
    // await deleteListingByName(client, 'Cozy Cottage');

    //deleteMany
    // await deleteListingScrapedBeforeDate(client, new Date('2019-02-15'));

  } catch (err) {
    console.error(err);
  } finally {
    await client.close()
  }
};

main().catch(console.error);

//Gerar lista dbs disponíveis
async function listDatabases(client) {
  const databasesList = await client.db().admin().listDatabases();

  console.log('Databases:');
  databasesList.databases.forEach(db => {
    console.log(`- ${db.name}`);
  })
};

//CRUD

//createOne
async function createListing(client, newListing) {
  const result = await client.db('sample_airbnb').collection('listingsAndReviews')
    .insertOne(newListing);

  console.log(`New listing created with the following id: ${result.insertedId}`);
};

//createMany
async function createMultipleListings(client, newListings) {
  const result = await client.db('sample_airbnb').collection('listingsAndReviews')
    .insertMany(newListings);

  console.log(`${result.insertedCount} new listing created with the following id(s):`);

  console.log(result.insertedIds);
};

//findOne
async function findOneListingByName(client, nameOfListing) {
  const result = await client.db('sample_airbnb').collection('listingsAndReviews')
    .findOne({ name: nameOfListing });

  if (result) {
    console.log(`Found a listing in the collection with the name '${nameOfListing}'`);
    console.log(result);
  } else {
    console.log(`No listing found with the name '${nameOfListing}'`);
  }
};

//findMany
async function findListingsWithMinimumBedroomsBathroomsAndMostRecentReviews(client, {
  minimumNumberOfBedrooms = 0,
  minimumNumberOfBathrooms = 0,
  maximumNumberOfResults = Number.MAX_SAFE_INTEGER
} = {}) {
  const cursor = client.db('sample_airbnb').collection('listingsAndReviews').find({
    bedrooms: { $gte: minimumNumberOfBedrooms },
    bathrooms: { $gte: minimumNumberOfBathrooms }
  }).sort({ last_review: -1 })
    .limit(maximumNumberOfResults);

  const results = await cursor.toArray()

  if (results.length > 0) {
    console.log(`Found listing(s) with at least ${minimumNumberOfBedrooms} bedrooms and ${minimumNumberOfBathrooms} bathrooms:`);
    results.forEach((result, i) => {
      date = new Date(result.last_review).toDateString();
      console.log();
      console.log(`${i + 1}. name: ${result.name}`);
      console.log(`_id: ${result._id}`);
      console.log(` bedrooms: ${result.bedrooms}`);
      console.log(` bathrooms: ${result.bathrooms}`);
      console.log(` most recent review date: ${new Date(result.last_review)
        .toDateString()}`);
    });
  }
};

//updateOne
async function updateListingByName(client, nameOfListing, updateListing) {
  const result = await client.db('sample_airbnb').collection('listingsAndReviews')
    .updateOne({ name: nameOfListing }, { $set: updateListing });

  console.log(`${result.matchedCount} documents(s) matched the query criteria`);
  console.log(`${result.modifiedCount} documents was/were update`);
};

//upsert
async function upsertListingByName(client, nameOfListing, updateListing) {
  const result = await client.db('sample_airbnb').collection('listingsAndReviews')
    .updateOne({ name: nameOfListing }, { $set: updateListing }, { upsert: true });

  console.log(`${result.matchedCount} documents(s) matched the query criteria`);

  if (result.upsertedCount > 0) {
    console.log(`One document was inserted with the id ${result.upsertedId}`)
  } else {
    console.log(`${result.modifiedCount} document(s) was/were updated`);
  }
};

//updateMany
async function updateAllListingsToHavePropertyType(client) {
  const result = await client.db('sample_airbnb').collection('listingsAndReviews')
    .updateMany({
      property_type: { $exists: false }
    },
      {
        $set: { property_type: 'Unknown' }
      });

  console.log(`${result.matchedCount} document(s) matched the query criteria`);
  console.log(`${result.modifiedCount} document(s) was/were updated`);
};

//deleteOne
async function deleteListingByName(client, nameOfListing) {
  const result = await client.db('sample_airbnb').collection('listingsAndReviews')
    .deleteOne({ name: nameOfListing });

  console.log(`${result.deletedCount} documents was/were deleted`);
};

//deleteMany
async function deleteListingScrapedBeforeDate(client, date) {
  const result = await client.db('sample_airbnb').collection('listingsAndReviews')
    .deleteMany({ 'last_scraped': { $lt: date } });

  console.log(`${result.deletedCount} documents was/were deleted`);
};