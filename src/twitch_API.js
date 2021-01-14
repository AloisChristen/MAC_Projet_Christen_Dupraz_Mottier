const ApiClient = require('twitch');
const ClientCredentialsAuthProvider =  require('twitch-auth');



class twitch_API {

    constructor() {
        const clientId = process.env.TWITCH_CLIENT_ID;
        const clientSecret = process.env.TWITCH_CLIENT_SECRET;
        const authProvider = new ClientCredentialsAuthProvider.ClientCredentialsAuthProvider(clientId, clientSecret);
        this.apiClient = new ApiClient({ authProvider });
    }

    async getTopGames(number = 10){
        //TODO return more games
        let paginator = this.apiClient.helix.games.getTopGamesPaginated();
        let top_games = [];
        console.log("Fetching top games streamed");
        for await (const games of paginator){
            top_games = top_games.concat(games);
            if(top_games.length > number){
                break
            }
        }
        console.log("Games retrieved from Twitch : ");
        console.log(top_games.map((g) => g.name));
        return top_games;
        // return this.apiClient.helix.games.getTopGames(paginator);
    }


}

module.exports = twitch_API;

