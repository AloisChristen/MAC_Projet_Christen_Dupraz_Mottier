const dotenv = require('dotenv');
const parse = require('csv-parse');
const fs = require('fs').promises;
const cliProgress = require('cli-progress');
const { join } = require('path');

const DocumentDAO = require('./DocumentDAO');
const GraphDAO = require('./GraphDAO');
const Twitch_API = require('./twitch_API');

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
  fs.readFile(join(__dirname, '../data/twitch_games.csv')).then((baseGames) => {
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
const twitch_API = new Twitch_API();


async function emptyMongo() {
  console.log("Empty MondoDb");

  let collections = await documentDAO.db.listCollections().toArray();
  for await (const collection of collections){
    console.log("Dropping " + collection.name);
    documentDAO.db.dropCollection(collection.name);
  }
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


async function parseGame() {
  console.log('Parsing CSV');
  const parseGamesBar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic);
  let parsedGames = await parseGames();
  console.log("Writing games to mongo");
  parseGamesBar.start(parsedGames.length, 9);
  // games_old.csv format
  /*/
  return Promise.all(parsedGames.slice(1).map((it) => {
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
    documentDAO.insertGame({
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
  /**/
  // twitch selected games CSV format
  /**/
  return Promise.all(parsedGames.slice(1).map((it) => {
    const [
      _id,
      basename,
      name,
      year,
      platforms,
      genres,
      critic_score,
      user_score
    ] = it;
    documentDAO.insertGame({
      _id,
      basename,
      name,
      year,
      platforms,
      genres,
      critic_score,
      user_score
     }).then(() => parseGamesBar.increment());
    /**/
  })).then(() => {
    parseGamesBar.stop();
  });
}

async function loadGames() {
  // Load them back to get their id along
  console.log('Loading games back in memory');
  return await documentDAO.getAllGames();
}


function calculateGenreAndPlatForms(games) {
  // Retrieve all genres and platforms from all games, split them and assign a numeric id
  console.log('Calculating genres and platforms');
  function splitAndGroup(objList, property){
    return [... new Set(objList.flatMap((it) =>
        it[property].split(',').map(it => it.trim())))].map((it, i) => [i,it]);
  }
  const genres = splitAndGroup(games, "genres");
  const platforms = splitAndGroup(games, "platforms");
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
    const gameGenres = game.genres.split(',').map(i => i.trim());
    const gamePlatforms = game.platforms.split(',').map(i => i.trim());
    graphDAO.upsertGame(game._id, game.basename).then(() => {

      // Update platform <-> game links
      Promise.all(gamePlatforms.map((name) => {
        const id = platforms.find((it) => it[1] === name)[0];
        return graphDAO.upsertPlatform(game._id, { id, name });
      })).then(() => {
        // Update genre <-> game links
        Promise.all(gameGenres.map((name) => {
          const id = genres.find((it) => it[1] === name)[0];
          return graphDAO.upsertGenre(game._id, {id, name});
        })).then(() => {
          gamesBar.increment();
          resolve1();
        });
      });
    });
  }).then(() => {
  gamesBar.stop();
}))).then(() => {
  console.log("Data loaded");
  });
}

async function addData() {
  await writeUsers();
  await parseGame();
  let games = await loadGames();
  let data = calculateGenreAndPlatForms(games);
  let genres = data.genres;
  console.log("Genres : " + genres);
  let platforms = data.platforms;
  console.log("Platforms : " + platforms);
  await insertInNeo4j(games, genres, platforms);
  // await loadGamesFromTwitch().then(() => console.log("Finish loading games from Twitch"));
  // await selectTwitchGames(5).then(()=> {console.log("finish selecting")});
}

async function selectTwitchGames(nb) {
  let selectedGames = [];
  for (; selectedGames.length < nb;) {
    let nextBatch = await twitch_API.getNextGames();
    for (const twitchGame of nextBatch) {
      let gamesFound = await documentDAO.getStrictGames(twitchGame.name);
      if (gamesFound.length == 1) {
        let selected = {
          games: gamesFound[0],
          twichName: twitchGame.name,
          twitchId : twitchGame.id
        }
        if (selectedGames.find(sel => sel.twitchId === selected.twitchId) === undefined){
          selectedGames.push(selected);
          if (selectedGames.length % 10 === 0) {
            console.log(selectedGames.length);
          }
        }
      }
    }
  }
  return selectedGames;
}

async function loadGamesFromTwitch() {
  let twitchGames = await twitch_API.getTopGames(1000);
  let nb_found_games = 0;
    twitchGames.forEach((twitchGame) => {
      documentDAO.getStrictGames(twitchGame.name).then((gamesFound) => {
        if(gamesFound.length > 0){
          nb_found_games++;
          if(nb_found_games % 10 === 0){
            console.log(nb_found_games);
          }
          // console.log("Games found for " + twitchGame.name);
          // console.log(gamesFound);
          // twitchGame.getStreams().then((streams) => {
          //   streams.data.forEach((stream) => {
          //     // console.log(stream.userDisplayName + " : " + stream.title);
          //    stream.getUser().then((streamer) => {
          //      documentDAO.insertStreamer({
          //        displayName: streamer.displayName,
          //        name: streamer.name,
          //        _id: streamer.id,
          //        language: stream.language,
          //      });
          //    });
          //   })
          // })
        }

      })
    })
}

async function writeCSV(games){
  let columnsName = ["id", "basename", "name", "year", "platform", "genres", "critic_score", "user_score"];
  let csvContent = "";
  games = games.map((g) => {
    let platforms = "\"" + g.games.platforms.join(",") + "\"";
    let genres = "\"" + g.games.genres.join(",") + "\"";
    return [
        g.twitchId, g.games.basename, g.twichName,
        g.games.year, platforms, genres,
        g.games.critic_score, g.games.user_score];
  });
  csvContent += columnsName.join(",") + "\r\n";
  games.forEach(function(rowArray) {
    let row = rowArray.join(",");
    csvContent += row + "\r\n";
  });
  console.log(csvContent);
  await fs.writeFile(join(__dirname, "../data/twitch_games.csv"), csvContent, 'utf8')
      .catch(err => {console.log(err.message);});
}

// MAIN
async function main() {
  console.log('Starting mongo');
  await documentDAO.init();
  await emptyMongo();
  console.log('Preparing Neo4j');
  await graphDAO.prepare();
  await emptyNeo4j();
  await addData();
  // let selection = await selectTwitchGames(500);
  // await writeCSV(selection);
}

main().then(() => {
  console.log("End");
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




