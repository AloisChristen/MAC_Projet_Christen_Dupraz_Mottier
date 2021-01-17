const dotenv = require('dotenv');
const parse = require('csv-parse');
const fs = require('fs').promises;
const cliProgress = require('cli-progress');
const { join } = require('path');

const DocumentDAO = require('./DocumentDAO');
const Twitch_API = require('./twitch_API');

const twitchGamesCSV = join(__dirname, '../data/twitch_games2.csv');
const allGamesCSV = join(__dirname, '../data/games_old.csv');

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
    let selection = await selectTwitchGames(500);
    await writeCSV(selection);
}

main().then(() => {
    console.log("End");
});



