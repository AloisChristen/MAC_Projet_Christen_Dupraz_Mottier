const { MongoClient } = require('mongodb');

class DocumentDAO {

  constructor() {
    this.client = null;
    this.db = null;
    this.collection = null;
  }

  init() {
    return new Promise((resolve) => {
      MongoClient.connect(`mongodb://root:toor@${process.env.DOCUMENTDB_HOST}/?authSource=admin`, (err, client) => {
        if (err !== null) throw err;
        this.client = client;
        this.db = client.db(process.env.DOCUMENTDB_NAME);
        this.collection = this.db.collection('mac2020');
        resolve(null);
      });
    });
  }

  close() {
    return this.client.close();
  }

  insertGame(game) {
    return this.collection.insertOne(game);
  }

  getGames(search) {

    return this.collection.aggregate([
      { $match: { 'name': new RegExp(search, "gi") }},
      { $limit: 10},
      { $group: { _id: "$name",
                  platform: {$addToSet: "$platform"},
                  _year: { $min: "$year"},
                  genres: {$addToSet: "$genre"}
      }},
      { $limit: 10}
    ]).toArray();
  }

  getGameById(id) {
    return this.collection.findOne({ _id: id });
  }

  getRandomGames(n) {
    return this.collection.find().limit(n).toArray();
  }

  getAllGames() {
    return this.collection.find().toArray().then((result) => {
      return result.map((it) => ({
        ...it,
        _id: it._id.toString()
      }));
    });
  }
}

module.exports = DocumentDAO;
