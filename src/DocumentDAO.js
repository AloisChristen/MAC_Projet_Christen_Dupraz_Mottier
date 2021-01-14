const { MongoClient } = require('mongodb');

class DocumentDAO {

  constructor() {
    this.client = null;
    this.db = null;
    this.gameCollection = null;
  }

  init() {
    return new Promise((resolve) => {
      MongoClient.connect(`mongodb://root:toor@${process.env.DOCUMENTDB_HOST}/?authSource=admin`, (err, client) => {
        if (err !== null) throw err;
        this.client = client;
        this.db = client.db(process.env.DOCUMENTDB_NAME);
        this.gameCollection = this.db.collection('games');
        this.streamerCollection = this.db.collection('streamers');
        resolve(null);
      });
    });
  }

  close() {
    return this.client.close();
  }

  insertGame(game) {
    return this.gameCollection.insertOne(game);
  }

  insertStreamer(streamer){
    return this.streamerCollection.insertOne(streamer);
  }

  getGames(search) {

    return this.gameCollection.aggregate([
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

  getStrictGames(search) {
    let reg = "^" + search + "$";
    return this.gameCollection.aggregate([
      { $match: { 'name': new RegExp(reg, 'gi') }},
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
    return this.gameCollection.findOne({ _id: id });
  }

  getRandomGames(n) {
    return this.gameCollection.find().limit(n).toArray();
  }

  async getAllGames() {
    let games = await this.gameCollection.find().toArray();
    return games.map((it) => ({
      ...it,
      _id: it._id.toString()
    }));
  }
}

module.exports = DocumentDAO;
