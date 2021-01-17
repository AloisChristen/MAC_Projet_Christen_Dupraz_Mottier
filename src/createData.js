const dotenv = require('dotenv');
const parse = require('csv-parse');
const fs = require('fs').promises;
const cliProgress = require('cli-progress');
const { join } = require('path');

const DocumentDAO = require('./DocumentDAO');
const Twitch_API = require('./twitch_API');


const allGamesCSV = join(__dirname, '../data/games_old.csv');
const twitchGamesCSV = join(__dirname, '../data/twitch_games.csv');
const twitchStreamerCSV = join(__dirname, '../data/twitch_streamers.csv');

dotenv.config();

const parseCSV = (csvPath) => new Promise((resolve) => {
    fs.readFile(csvPath).then((baseGames) => {
        parse(baseGames, (err, data) => {
            if (err !== undefined) console.log("Errors while parsing : " + err);
            resolve(data);
        });
    });
});

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

async function parseAllGames() {
    console.log('Parsing CSV');
    let parsedGames = await parseCSV(allGamesCSV);
    console.log("Writing games to mongo");
    const parseGamesBar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic);
    parseGamesBar.start(parsedGames.length, 0);
    // games_old.csv format
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
    })).then(() => {
        parseGamesBar.stop();
    });
}

async function addBasicData() {
    await parseAllGames();
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



async function selectTwitchStreamers(nb, allGames){
    return await loadStreamerFromGames(allGames.slice(0, nb), 3, allGames);
}

async function loadStreamerFromGames(games, max_streamers, allGames){
    let streamers = []
    for await (const game of games){
        streamers = streamers.concat(await loadStreamerFromGame(game, max_streamers, allGames));
    }
    return streamers;
}

async function loadStreamerFromGame(game, max_streamers, allGames){
    console.log("Searching streamers for game " + game.name)
    let streamers = [];
    let twitchGame = await twitch_API.getGame(game);
    let streams = await twitchGame.getStreams();
    let total_streamer = 0;
    await new Promise(resolve => setTimeout(resolve, 200));
    for await (const stream of streams.data){
        if(total_streamer > max_streamers) break;
        total_streamer++;
        let streamer = await stream.getUser();
        let streamerHash = {
            name: streamer.name,
            twitchId: streamer.id,
            // language: streamer.language,
            gamesPlayed: await getAllGamesPlayed(streamer, allGames, game),
        };
        streamers.push(streamerHash);
    }
    return streamers;
}

async function getAllGamesPlayed(streamer, allGames, current){
   let gamesPlayed = []
   let hashGames = await twitch_API.getAllGamesPlayed(streamer, 20);
   if (hashGames[current._id] === undefined)
       hashGames[current._id] = 0;
   hashGames[current._id]++;
   for(const id of Object.keys(hashGames)){
       let twitchGame = allGames.find((game) => game._id === id);
       if (twitchGame !== undefined){
           gamesPlayed.push({id: twitchGame._id, total: hashGames[id], name: twitchGame.name});
       }
   }
   return gamesPlayed;
}

async function writeStreamerCSV(streamers){
    let columnsName = ["id", "name", "language", "games_names", "play_count"];
    let csvContent = "";
    streamers = streamers.map((s) => {
        let games_names = "\"" + s.gamesPlayed.map(g => g.name).join(",") + "\"";
        let play_counts = "\"" + s.gamesPlayed.map(g => g.total).join(",") + "\"";

        return [
           s.twitchId, s.name, s.language, games_names, play_counts
        ];
    });
    csvContent += columnsName.join(",") + "\r\n";
    streamers.forEach(function(rowArray) {
        let row = rowArray.join(",");
        csvContent += row + "\r\n";
    });
    console.log(csvContent);
    await fs.writeFile(twitchStreamerCSV, csvContent, 'utf8')
        .catch(err => {console.log(err.message);});
}

async function writeGamesCSV(games){
    let columnsName = ["id", "basename", "name", "year", "platform", "genres", "critic_score", "user_score"];
    let csvContent = "";
    games = games.map((g) => {
        let platforms = "\"" + g.games.platforms.join(",") + "\"";
        let genres = "\"" + g.games.genres.join(",") + "\"";
        return [
            g.twitchId, g.games.basename, g.twichName,
            g.games.year, platforms, genres,
            g.games.critic_score, g.games.user_score
        ];
    });
    csvContent += columnsName.join(",") + "\r\n";
    games.forEach(function(rowArray) {
        let row = rowArray.join(",");
        csvContent += row + "\r\n";
    });
    console.log(csvContent);
    await fs.writeFile(twitchGamesCSV, csvContent, 'utf8')
        .catch(err => {console.log(err.message);});
}

async function prepareMongo() {
    console.log('Starting mongo');
    await documentDAO.init();
    await emptyMongo();
    await addBasicData();
}

// MAIN
async function main() {
   await prepareMongo();
   // Creating TwitchGames CSV;
    let gamesSelection = await selectTwitchGames(500);
    // await writeGamesCSV(gamesSelection);
    let streamersSelection = await selectTwitchStreamers(100, gamesSelection);
    await writeStreamerCSV(streamersSelection);
}

main().then(() => {
    console.log("End");
});



