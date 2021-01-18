const dotenv = require('dotenv');
const parse = require('csv-parse');
const fs = require('fs').promises;
const cliProgress = require('cli-progress');
const { join } = require('path');

const DocumentDAO = require('./DocumentDAO');
const GraphDAO = require('./GraphDAO');
const Twitch_API = require('./twitch_API');

const twitchGamesCSV = join(__dirname, '../data/twitch_games.csv');
const twitchStreamerCSV = join(__dirname, '../data/twitch_streamers.csv');

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

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

  for (let i = array.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * i);
    const temp = array[i];
    array[i] = array[j];
    array[j] = temp;
  }

  return array;
};

const parseCSV = (csvPath) => new Promise((resolve) => {
  fs.readFile(csvPath).then((baseGames) => {
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
  buildUser(136415864, 'GTZL1', 'david', 'dupraz', 'fr', false),
];

const graphDAO = new GraphDAO();
const documentDAO = new DocumentDAO();
const twitch_API = new Twitch_API();


async function emptyMongo() {
  console.log("Empty MondoDb");

  let collections = await documentDAO.db.listCollections().toArray();
  for await (const collection of collections) {
    console.log("Dropping " + collection.name);
    documentDAO.db.dropCollection(collection.name);
  }
}

async function emptyNeo4j() {
  console.log("Empty Neo4j");
  await graphDAO.run("match (a) -[r] -> () delete a, r");
  await graphDAO.run("MATCH (a) delete a");
}

async function writeUsers() {
  console.log('Writing users to neo4j');
  await Promise.all(users.map((user) => graphDAO.upsertUser(user)));
}

async function parseGames() {
  console.log('Parsing CSV');
  let parsedGames = await parseCSV(twitchGamesCSV);
  console.log("Writing games to mongo");
  const parseGamesBar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic);
  parseGamesBar.start(parsedGames.length, 0);

  // twitch games CSV format
  /**/
  await Promise.all(parsedGames.slice(1).map((it) => {
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

async function parseStreamers() {
  console.log('Parsing CSV');
  let parsedStreamers = await parseCSV(twitchStreamerCSV);
  console.log("Writing Streamer to mongo");
  const parseStreamersBar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic);
  parseStreamersBar.start(parsedStreamers.length, 0);

  // twitch games CSV format
  /**/
  await Promise.all(parsedStreamers.slice(1).map((it) => {
    const [
      id, name, language, games_played, plays_count
    ] = it;
    documentDAO.insertStreamer({
      id, name, games_played, plays_count
    }).then(() => parseStreamersBar.increment());
    /**/
  })).then(() => {
    parseStreamersBar.stop();
  });
}

async function loadGames() {
  // Load them back to get their id along
  console.log('Loading games back in memory');
  return await documentDAO.getAllGames();

}

async function loadStreamers() {
  return await documentDAO.getAllStreamers();
}


function calculateGenreAndPlatForms(games) {
  // Retrieve all genres and platforms from all games, split them and assign a numeric id
  console.log('Calculating genres and platforms');
  function splitAndGroup(objList, property) {
    return [... new Set(objList.flatMap((it) =>
      it[property].split(',').map(it => it.trim())))].map((it, i) => [i, it]);
  }
  const genres = splitAndGroup(games, "genres");
  const platforms = splitAndGroup(games, "platforms");
  return {
    genres: genres,
    platforms: platforms
  };
}

function insertGamesInNeo4j(games, genres, platforms) {
  console.log('Handling game insertion in Neo4j');
  const gamesBar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic);
  gamesBar.start(games.length, 0);
  return Promise.all(games.map((game) => new Promise((resolve1) => {
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
          return graphDAO.upsertGenre(game._id, { id, name });
        })).then(() => {
          gamesBar.increment();
          resolve1();
        });
      });
    });
  }).then(() => {
    gamesBar.stop();
  }))).then(() => {
    console.log("Games loaded");
  });
}

async function insertStreamerInNeo4j(streamers) {
  console.log('Handling streamers insertion in Neo4j');
  return Promise.all(streamers.map((streamer) => new Promise(async (resolve1) => {
    const gamesNames = streamer.games_played.split(',').map(i => i.trim());
    const playsCount = streamer.plays_count.split(',').map(i => i.trim());
    for (let i in gamesNames) {
      console.log("Inserting streamer : " + streamer.name);
      console.log(gamesNames[i]);
      console.log(playsCount[i]);
      await graphDAO.upsertStreamer(gamesNames[i], streamer, playsCount[i])
    }
  }))).then(() => {
    console.log("Streamers loaded");
  });
}

async function addData() {
  await writeUsers();
  await parseGames();
  await parseStreamers();
  let games = await loadGames();
  let streamers = await loadStreamers();
  let data = calculateGenreAndPlatForms(games);
  let genres = data.genres;
  console.log("Genres : " + genres);
  let platforms = data.platforms;
  console.log("Platforms : " + platforms);
  await insertGamesInNeo4j(games, genres, platforms);
  await sleep(500);
  await addMoreData(games, platforms, genres);
  await sleep(500);
  await insertStreamerInNeo4j(streamers);
}

async function loadStreamerFromGames(games) {
  games.forEach((game) => loadStreamerFromGame(game));
}

async function loadFakeRelationGameStreamer() {
  documentDAO.getAllStreamers().then((streamer) => {
    documentDAO.getRandomGames(5).then((game) => graphDAO.upsertRelationGameStreamer(streamer.id, game._id));
  });
}

async function loadStreamerFromGame(game) {
  let twitchGame = await twitch_API.getGame(game);
  let streams = await twitchGame.getStreams();
  streams.data.forEach((stream) => {
    stream.getUser().then((streamer) => {
      documentDAO.insertStreamer({
        displayName: streamer.displayName,
        name: streamer.name,
        _id: streamer.id,
        language: streamer.language,
      });
      graphDAO.upsertStreamer(streamer.id, streamer.name, game._id).then(() => { });
    });
  });
}

// MAIN
async function main() {
  console.log('Starting mongo');
  await documentDAO.init();
  await emptyMongo();
  console.log('Preparing Neo4j');
  await graphDAO.prepare();
  await emptyNeo4j();
  await addData()
}

main().then(() => {
  console.log("End");
});


async function addMoreData(games, platforms, genres) {
  // Add some games added by users
  /*console.log('Add some games liked by users');
  const addedPromise = [400, 87, 0, 34, 58].flatMap((quantity, index) => {
    return shuffle(games).slice(0, quantity).map((game) => {
      return graphDAO.upsertAdded(users[index].id, game._id,
        { at: new Date(160613000 * 1000 + (Math.floor(Math.random() * 3124) * 1000)) });
    });
  });
  Promise.all(addedPromise).then(() => {*/
  // Add some games liked by users
  console.log('Add some games liked by users');
  const likePromise = [280, 34, 98, 254, 0, 129].flatMap((quantity, index) => {
    return shuffle(games).slice(0, quantity).map((game) => {
      return graphDAO.upsertGameLiked(users[index], game.basename, {
        rank: Math.floor(Math.random() * 5) + 1,
        at: new Date(160613000 * 1000 + (Math.floor(Math.random() * 3124) * 1000))
      });
    });
  });
  Promise.all(likePromise).then(() => {

    // Add some platforms liked by users
    console.log('Add some platforms liked by users');
    const plaformPromise = [300, 674, 0, 45, 36, 77].flatMap((quantity, index) => {
      return shuffle(platforms).slice(0, quantity).map((platform) => {
        let pName=platform[1];
        return graphDAO.upsertPlatformLiked(users[index], pName, {
          rank: Math.floor(Math.random() * 5) + 1,
          at: new Date(160613000 * 1000 + (Math.floor(Math.random() * 3124) * 1000))
        });
      });
    });
    Promise.all(plaformPromise).then(() => {
      /// Add some genres liked by users
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
        /*
        // Add some games requested
        console.log('Add some requested games');
        const requestedPromise = [560, 12, 456, 25, 387].flatMap((quantity, index) => {
          return shuffle(games).slice(0, quantity).map((game) => {
            return graphDAO.upsertRequested(users[index].id, game._id, {
              at: new Date(160613000 * 1000 + (Math.floor(Math.random() * 3124) * 1000))
            });
          });
        });*/
        //Promise.all(requestedPromise).then(() => {
        console.log('Done, closing sockets');
        Promise.all([
          documentDAO.close(),
          graphDAO.close()
        ]).then(() => {
          console.log('Done with importation');
          //});
        });
      });
    });
  });
  //});
}



