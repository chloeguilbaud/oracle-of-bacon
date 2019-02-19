const mongodb = require('mongodb');
const csv = require('csv-parser');
const fs = require('fs');

const MongoClient = mongodb.MongoClient;
const mongoUrl = 'mongodb://localhost:27017/workshop';

const insertActors = (db, callback) => {
    const collection = db.collection('actors');

    const actors = [];
    fs.createReadStream('./actors.csv')
        .pipe(csv())
        .on('data', data => {
            actors.push({
                "name:ID": data.name
            });
        })
        // A la fin on créé l'ensemble des acteurs dans MongoDB
        .on('end', () => {
            collection.insertMany(actors, (err, result) => {
                callback(result);
            });
        });
}

MongoClient.connect(mongoUrl, (err, db) => {
    insertActors(db, result => {
        console.log(`${result.insertedCount} actors inserted`);
        db.close();
    });
});