const ApiClient = require('twitch');
const ClientCredentialsAuthProvider =  require('twitch-auth');
//const { HelixStreamApi } = require('twitch/lib/API/Helix/Stream/HelixStreamApi');

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

class twitch_API {

    constructor() {
        const clientId = process.env.TWITCH_CLIENT_ID;
        const clientSecret = process.env.TWITCH_CLIENT_SECRET;
        const authProvider = new ClientCredentialsAuthProvider.ClientCredentialsAuthProvider(clientId, clientSecret);
        this.apiClient = new ApiClient({ authProvider });
        this.resetPagination();
    }

    async getVideos(game = null){
        return this.apiClient.kraken.videos.getTopVideos(game);
    }

    async getGame(game){
        return this.apiClient.helix.games.getGameByName(game.name);
    }

    async getTopGames(number = 10){
        //TODO return more games
        this.resetPagination();
        let top_games = [];
        for await (const games of this.paginator){
            console.log("Number of fetched games : " + top_games.length);
            top_games = top_games.concat(games);
            if(top_games.length > number){
                break;
            }
        }
        console.log("Games retrieved from Twitch : ");
        console.log(top_games.map((g) => g.name));
        return top_games;
        // return this.apiClient.helix.games.getTopGames(paginator);
    }

    resetPagination(){
        this.paginator = this.apiClient.helix.games.getTopGamesPaginated();
    }

    async getNextGames(){
        await sleep(500);
        return this.paginator.getNext();
    }

    async getStreamers(idGame){
        let game = await this.apiClient.helix.games.getGameByName(idGame);
        let streams = await game.getStreams();
        
        return streams.data;
    }

    async getAllGamesPlayed(streamer, nb = 20){
        let clips = await this.apiClient.helix.clips.getClipsForBroadcaster(streamer.id);
        let hashOfGames = clips.data.map(g => g.gameId).slice(0,nb).reduce(function(h, id){
            if(h[id] === undefined){
                h[id] = 1;
            } else if(id.trim() !== ''){ // On évite les résultats NULL
                h[id] += 1;
            }
            return h;
        }, {});
        return hashOfGames;
    }
}

module.exports = twitch_API;
