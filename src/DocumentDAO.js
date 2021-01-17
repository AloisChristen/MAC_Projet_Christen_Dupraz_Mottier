const { MongoClient } = require('mongodb');

class DocumentDAO {

  constructor() {
    this.client = null;
    this.db = null;
    this.gameCollection = null;
  }

  async init() {
    let connection =  new Promise((resolve) => {
      MongoClient.connect(`mongodb://root:toor@${process.env.DOCUMENTDB_HOST}/?authSource=admin`, (err, client) => {
        if (err !== null) throw err;
        this.client = client;
        this.db = client.db(process.env.DOCUMENTDB_NAME);
        this.gameCollection = this.db.collection('games');
        this.streamerCollection = this.db.collection('streamers');
        resolve(null);
      });
    });
    await connection.catch(err => {
      console.log("documentDao.init err : " + err.message);
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

  async getStrictGames(search) {
    let reg = "^" + search + "$";
    return this.gameCollection.aggregate([
      { $match: { 'name': new RegExp(reg, 'gi') }},
      { $limit: 10},
      { $group: {
          _id: "$basename",
          basename: {$first: "$basename"},
          name: {$first : "$name"},
          platforms: {$addToSet: "$platform"},
          year: { $min: "$year"},
          genres: {$addToSet: "$genre"},
          critic_score: { $max: "$critic_score"},
          user_score: {$max: "$user_score"}
        }
      },
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

  async getAllStreamers() {
    let streamers = await this.streamerCollection.find().toArray();
    return streamers.map((it) => ({
      ...it,
      _id: it._id.toString()
    }));
  }
}

module.exports = DocumentDAO;
