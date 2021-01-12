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

const parseGames = () => new Promise((resolve) => {
  fs.readFile(join(__dirname, '../data/games_old.csv')).then((baseGames) => {
    parse(baseGames, (err, data) => {
      if (err !== undefined) console.log("Errors while parsing : " + err);
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

function emptyMongo() {
  console.log("Empty MondoDb");

  documentDAO.collection.drop().then(() => {
    return documentDAO;
  });
}

function emptyNeo4j() {
  console.log("Empty Neo4j");
  return graphDAO.run("match (a) -[r] -> () delete a, r")
      .then(() => { graphDAO.run("MATCH (a) delete a"); });
}

function writeUsers() {
  console.log('Writing users to neo4j');
  return Promise.all(users.map((user) => graphDAO.upsertUser(user)));
}

function parseGame() {
  console.log('Parsing CSV and writing games to mongo');
  const parseGamesBar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic);
  parseGames().then((parsedGames) => {
    parseGamesBar.start(parsedGames.length, 0);
    Promise.all(parsedGames.slice(1).map((it) => {
      const [
        basename,
        name,
        genre,
        platform,
        publisher,
        developer,
        critic_score,
        user_score,
        year
      ] = it;
      return documentDAO.insertGame({
        basename,
        name,
        genre,
        platform,
        publisher,
        developer,
        critic_score,
        user_score,
        year
      }).then(() => parseGamesBar.increment());
    })).then(() => {
      parseGamesBar.stop();
      return parsedGames;
    });
  });
}

function loadGames() {
  // Load them back to get their id along
  console.log('Loading games back in memory');
  return documentDAO.getAllGames();
}

function calculateGenreAndPlatForms(games) {
  // Retrieve all genres and platforms from all games, split them and assign a numeric id
  console.log('Calculating genres and platforms');
  const genres = [...new Set(games.flatMap((it) => it.genre.split(',').map(it => it.trim())))].map((it, i) => [i, it]);
  const platforms = [...new Set(games.flatMap((it) =>
      it.platform.split(',').map(it => it.trim())))].map((it, i) => [i, it]);
  return {
    genres: genres,
    platforms: platforms
  };
}

function insertInNeo4j(games, genres, platforms){
  console.log('Handling game insertion in Neo4j');
  const gamesBar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic);
  gamesBar.start(games.length, 0);
  Promise.all(games.map((game) => new Promise((resolve1) => {
    const gameGenres = game.genre.split(',').map(i => i.trim());
    const gamePlatforms = game.platform.split(',').map(i => i.trim());

    graphDAO.upsertGame(game._id, game.basename).then(() => {

      // Update actor <-> game links
      // Promise.all(gamePlatforms.map((name) => {
      //   const id = platforms.find((it) => it[1] === name)[0];
      //   return graphDAO.upsertPlatform(game._id, { id, name });
      //})).then(() => {

      // Update genre <-> game links
      Promise.all(gameGenres.map((name) => {
        const id = genres.find((it) => it[1] === name)[0];
        return graphDAO.upsertGenre(game._id, {id, name});
      })).then(() => {
        gamesBar.increment();
        resolve1();
      });
    });
  }).then(() => {
    gamesBar.stop();
  })));
}

function addData() {
  writeUsers().then(() => {
    parseGame();
    loadGames().then((games) => {
      let data = calculateGenreAndPlatForms(games);
      let genres = data.genres;
      let platforms = data.platforms;
      insertInNeo4j(games, genres, platforms);
    });
  });
}

// MAIN
console.log('Starting mongo');
documentDAO.init().then(() => {

  emptyMongo();
  console.log('Preparing Neo4j');
  graphDAO.prepare().then(() => {

    emptyNeo4j().then(() => {
      addData();
    });
  });
});







            //
            //   // Add some films added by users
            //   console.log('Add some films liked by users');
            //   const addedPromise = [400, 87, 0, 34, 58].flatMap((quantity, index) => {
            //     return shuffle(games).slice(0, quantity).map((game) => {
            //       return graphDAO.upsertAdded(users[index].id, game._id, {
            //         at: new Date(160613000 * 1000 + (Math.floor(Math.random() * 3124) * 1000))
            //       });
            //     });
            //   });
            //   Promise.all(addedPromise).then(() => {
            //
            //     // Add some games liked by users
            //     console.log('Add some games liked by users');
            //     const likePromise = [280, 34, 98, 254, 0].flatMap((quantity, index) => {
            //       return shuffle(games).slice(0, quantity).map((game) => {
            //         return graphDAO.upsertGameLiked(users[index], game._id, {
            //           rank: Math.floor(Math.random() * 5) + 1,
            //           at: new Date(160613000 * 1000 + (Math.floor(Math.random() * 3124) * 1000))
            //         });
            //       });
            //     });
            //     Promise.all(likePromise).then(() => {
            //
            //       // Add some actors liked by users
            //       console.log('Add some actors liked by users');
            //       const actorsPromise = [300, 674, 0, 45, 36].flatMap((quantity, index) => {
            //         return shuffle(actors).slice(0, quantity).map(([actorId]) => {
            //           return graphDAO.upsertActorLiked(users[index].id, actorId, {
            //             rank: Math.floor(Math.random() * 5) + 1,
            //             at: new Date(160613000 * 1000 + (Math.floor(Math.random() * 3124) * 1000))
            //           });
            //         });
            //       });
            //       Promise.all(actorsPromise).then(() => {
            //         // Add some genres liked by users
            //         console.log('Add some genres liked by users');
            //         const genrePromise = [22, 3, 0, 4, 7].flatMap((quantity, index) => {
            //           return shuffle(genres).slice(0, quantity).map(([genreId, actor]) => {
            //             return graphDAO.upsertGenreLiked(users[index].id, genreId, {
            //               rank: Math.floor(Math.random() * 5) + 1,
            //               at: new Date(160613000 * 1000 + (Math.floor(Math.random() * 3124) * 1000))
            //             });
            //           });
            //         });
            //         Promise.all(genrePromise).then(() => {
            //           // Add some games requested
            //           console.log('Add some requested games');
            //           const requestedPromise = [560, 12, 456, 25, 387].flatMap((quantity, index) => {
            //             return shuffle(games).slice(0, quantity).map((game) => {
            //               return graphDAO.upsertRequested(users[index].id, game._id, {
            //                 at: new Date(160613000 * 1000 + (Math.floor(Math.random() * 3124) * 1000))
            //               });
            //             });
            //           });
            //           Promise.all(requestedPromise).then(() => {
            //             console.log('Done, closing sockets');
            //             Promise.all([
            //               documentDAO.close(),
            //               graphDAO.close()
            //             ]).then(() => {
            //               console.log('Done with importation');
            //             });
            //           });
            //         });
            //       });
            //     });
            //   });




