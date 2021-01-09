import { Collection, Db, MongoClient } from "mongodb";
import { Movie } from "./Model";

class DocumentDAO {

  private client: MongoClient;

  private db: Db;

  private collection: Collection;

  async init(): Promise<any> {
    return new Promise((resolve) => {
      MongoClient.connect(`mongodb://${process.env.DOCUMENTDB_HOST}`, (err, client) => {
        if (err !== null) throw err;
        this.client = client;
        this.db = client.db(process.env.DOCUMENTDB_NAME);
        this.collection = this.db.collection('movies-mac');
        resolve(null);
      });
    });
  }

  async close() {
    await this.client.close();
  }

  async insertMovie(movie: Partial<Movie>) {
    await this.collection.insertOne(movie);
  }

  async getMovies(search: string): Promise<Movie[]> {
    return await this.collection.find({ 'title': new RegExp(search) }).limit(10).toArray();
  }

  async getMovieById(id: string) {
    return await this.collection.findOne({ _id: id });
  }

  async getRandomMovies(n: number) {
    return await this.collection.find().limit(n).toArray();
  }

  async getAllMovies(): Promise<Movie[]> {
    return (await this.collection.find().toArray()).map((it) => ({
      ...it,
      _id: it._id.toString()
    }));
  }
}

export default DocumentDAO;