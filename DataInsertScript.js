// const { Client } = require("pg");
const mongoose = require('mongoose');
require('dotenv').config();
const fs = require('fs');
const JSONStream = require('JSONStream');
const radash = require('radash');
// const pgClient = new Client({
//   user: "your_user",
//   host: "your_postgres_host",
//   database: "your_database",
//   password: "your_password",
//   port: 5432,
// });

const mongoURI = process.env.MONGO_URI;
console.log(mongoURI);

// Your collection name
const collectionName = "load_New";

// Define a schema for the collection
const Schema = mongoose.Schema;
const itemSchema = new Schema({}, { strict: false });
const collection = mongoose.model(collectionName, itemSchema);


const batchSize = 4;   // Define How many Records you want to insert 10000,50000 whatever
let batch = [];
let count = 0;
let totalReords = 0;

async function streamLoadToMongo() {
    try {

        //   await pgClient.connect();
        await mongoose.connect(mongoURI, { useNewUrlParser: true, useUnifiedTopology: true });
        console.log("Connected to MongoDB...");

        let started = Date.now();        
        // Add your json file and replace file name here
        const readStream = fs.createReadStream('one.json', { encoding: 'utf8' }).pipe(JSONStream.parse('*'));

        readStream.on('data', async (doc) => {
            batch.push(doc);
            if (batch.length >= batchSize) {
                totalReords = totalReords + batchSize;
                count++;
                readStream.pause(); 
                console.log('Batch No',count);                
                await collection.insertMany(batch);
                console.log(`Inserted ${batch.length} records`);
                batch = [];
                await radash.sleep(100);
                readStream.resume(); 
            }
        })

        readStream.on('end', async () => {
            console.log('stream ende');
            if (batch.length > 0) {
                totalReords = totalReords + batch.length;
                await collection.insertMany(batch);
                console.log(`Inserted final ${batch.length} records`);
            }
            console.log("Data transfer complete.");
            console.log('Total Records Inserted',totalReords);
            console.log('Operation took - ', (Date.now() - started) * 0.001, ' seconds\n');
        });

    }
    catch (err) {
        console.log(err);
    }
}

streamLoadToMongo().catch(console.error);



//9-3=2025
//KAL-EL
// For Reference
//https://medium.com/shelf-io-engineering/50-million-records-insert-in-mongodb-using-node-js-5c62b7d7af5a

