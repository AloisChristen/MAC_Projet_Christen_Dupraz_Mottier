const dotenv = require('dotenv');
const parse = require('csv-parse');
const fs = require('fs').promises;
const cliProgress = require('cli-progress');
const { join } = require('path');

const DocumentDAO = require('./DocumentDAO');
const GraphDAO = require('./GraphDAO');

dotenv.config();

const buildUser = (id, username, first_name, last_name, language_code, is_bot) => ({
  id,
  username,
  first_name,
  last_name,
  language_code,
  is_bot
});

const shuffle = (array) => {

  for(let i = array.length - 1; i > 0; i--){
    const j = Math.floor(Math.random() * i);
    const temp = array[i];
    array[i] = array[j];
    array[j] = temp;
  }

  return array;
};

const parseMovies = () => new Promise((resolve) => {
  fs.readFile(join(__dirname, '../data/movies.csv')).then((baseMovies) => {
    parse(baseMovies, (err, data) => {
      resolve(data);
    });
  });
});

const users = [
  buildUser(220987852, 'ovesco', 'guillaume', '', 'fr', false),
  buildUser(136451861, 'thrudhvangr', 'christopher', '', 'fr', false),
  buildUser(136451862, 'NukedFace', 'marcus', '', 'fr', false),
  buildUser(136451863, 'lauralol', 'laura', '', 'fr', false),
  buildUser(136451864, 'Saumonlecitron', 'jean-michel', '', 'fr', false),
];

const graphDAO = new GraphDAO();
const documentDAO = new DocumentDAO();


console.log('Starting mongo');
documentDAO.init().then(() => {

  console.log('Preparing Neo4j');
  graphDAO.prepare().then(() => {

    console.log('Writing users to neo4j');
    Promise.all(users.map((user) => graphDAO.upsertUser(user))).then(() => {

      console.log('Parsing CSV and writing movies to mongo');
      const parseMoviesBar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic);
      parseMovies().then((parsedMovies) => {
        parseMoviesBar.start(parsedMovies.length, 0);

        Promise.all(parsedMovies.map((it) => {
          const [
            rank, title, genre, description, director,
            actors, year, runtime, rating, votes,
            revenue, metascore
          ] = it;
          return documentDAO.insertMovie({
            rank, title, genre, description, director,
            actors, year, runtime, rating, votes,
            revenue, metascore
          }).then(() => parseMoviesBar.increment());
        })).then(() => {
          parseMoviesBar.stop();

          // Load them back to get their id along
          console.log('Loading movies back in memory');
          documentDAO.getAllMovies().then((movies) => {

            // Retrieve all genres and actors from all movies, split them and assign a numeric id
            console.log('Calculating genres and actors');
            const genres = [...new Set(movies.flatMap((it) => it.genre.split(',').map(it => it.trim())))].map((it, i) => [i, it]);
            const actors = [...new Set(movies.flatMap((it) => it.actors.split(',').map(it => it.trim())))].map((it, i) => [i, it]);
            
            console.log('Handling movie insertion in Neo4j');
            const moviesBar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic);
            moviesBar.start(movies.length, 0);

            Promise.all(movies.map((movie) => new Promise((resolve1) => {
              const movieGenres = movie.genre.split(',').map(i => i.trim());
              const movieActors = movie.actors.split(',').map(i => i.trim());

              graphDAO.upsertMovie(movie._id, movie.title).then(() => {

                // Update actor <-> movie links
                Promise.all(movieActors.map((name) => {
                  const id = actors.find((it) => it[1] === name)[0];
                  return graphDAO.upsertActor(movie._id, { id, name });
                })).then(() => {

                  // Update genre <-> movie links
                  Promise.all(movieGenres.map((name) => {
                    const id = genres.find((it) => it[1] === name)[0];
                    return graphDAO.upsertGenre(movie._id, { id, name });
                  })).then(() => {
                    moviesBar.increment();
                    resolve1();
                  });
                });
              });
            }))).then(() => {
              moviesBar.stop();

              // Add some films added by users
              console.log('Add some films liked by users');
              const addedPromise = [400, 87, 0, 34, 58].flatMap((quantity, index) => {
                return shuffle(movies).slice(0, quantity).map((movie) => {
                  return graphDAO.upsertAdded(users[index].id, movie._id, {
                    at: new Date(160613000 * 1000 + (Math.floor(Math.random() * 3124) * 1000)) 
                  });
                });
              });
              Promise.all(addedPromise).then(() => {

                // Add some movies liked by users
                console.log('Add some movies liked by users');
                const likePromise = [280, 34, 98, 254, 0].flatMap((quantity, index) => {
                  return shuffle(movies).slice(0, quantity).map((movie) => {
                    return graphDAO.upsertMovieLiked(users[index], movie._id, {
                      rank: Math.floor(Math.random() * 5) + 1,
                      at: new Date(160613000 * 1000 + (Math.floor(Math.random() * 3124) * 1000)) 
                    });
                  });
                });
                Promise.all(likePromise).then(() => {

                  // Add some actors liked by users
                  console.log('Add some actors liked by users');
                  const actorsPromise = [300, 674, 0, 45, 36].flatMap((quantity, index) => {
                    return shuffle(actors).slice(0, quantity).map(([actorId]) => {
                      return graphDAO.upsertActorLiked(users[index].id, actorId, {
                        rank: Math.floor(Math.random() * 5) + 1,
                        at: new Date(160613000 * 1000 + (Math.floor(Math.random() * 3124) * 1000))
                      });
                    });
                  });
                  Promise.all(actorsPromise).then(() => {
                    // Add some genres liked by users
                    console.log('Add some genres liked by users');
                    const genrePromise = [22, 3, 0, 4, 7].flatMap((quantity, index) => {
                      return shuffle(genres).slice(0, quantity).map(([genreId, actor]) => {
                        return graphDAO.upsertGenreLiked(users[index].id, genreId, {
                          rank: Math.floor(Math.random() * 5) + 1,
                          at: new Date(160613000 * 1000 + (Math.floor(Math.random() * 3124) * 1000))
                        });
                      });
                    });
                    Promise.all(genrePromise).then(() => {
                      // Add some movies requested
                      console.log('Add some requested movies');
                      const requestedPromise = [560, 12, 456, 25, 387].flatMap((quantity, index) => {
                        return shuffle(movies).slice(0, quantity).map((movie) => {
                          return graphDAO.upsertRequested(users[index].id, movie._id, {
                            at: new Date(160613000 * 1000 + (Math.floor(Math.random() * 3124) * 1000))
                          });
                        });
                      });
                      Promise.all(requestedPromise).then(() => {
                        console.log('Done, closing sockets');
                        Promise.all([
                          documentDAO.close(),
                          graphDAO.close()
                        ]).then(() => {
                          console.log('Done with importation');
                        });
                      });
                    });
                  });
                });
              });
            });
          });
        });
      });
    });
  });
});
