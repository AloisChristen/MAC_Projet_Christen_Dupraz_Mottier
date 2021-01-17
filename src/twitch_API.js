const ApiClient = require('twitch');
const ClientCredentialsAuthProvider =  require('twitch-auth');

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
}




module.exports = twitch_API;

