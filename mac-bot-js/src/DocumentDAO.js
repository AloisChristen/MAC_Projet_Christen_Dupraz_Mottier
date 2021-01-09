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

  insertMovie(movie) {
    return this.collection.insertOne(movie);
  }

  getMovies(search) {
    return this.collection.find({ 'title': new RegExp(search) }).limit(10).toArray();
  }

  getMovieById(id) {
    return this.collection.findOne({ _id: id });
  }

  getRandomMovies(n) {
    return this.collection.find().limit(n).toArray();
  }

  getAllMovies() {
    return this.collection.find().toArray().then((result) => {
      return result.map((it) => ({
        ...it,
        _id: it._id.toString()
      }));
    });
  }
}

module.exports = DocumentDAO;
