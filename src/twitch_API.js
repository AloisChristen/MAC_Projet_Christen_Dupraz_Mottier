const ApiClient = require('twitch');
const ClientCredentialsAuthProvider =  require('twitch-auth');



class twitch_API {

    constructor() {
        const clientId = process.env.TWITCH_CLIENT_ID;
        const clientSecret = process.env.TWITCH_CLIENT_SECRET;
        const authProvider = new ClientCredentialsAuthProvider.ClientCredentialsAuthProvider(clientId, clientSecret);
        this.apiClient = new ApiClient({ authProvider });
    }

    getTopGames(number = 10){
        //TODO return more games
        return this.apiClient.helix.games.getTopGames();
    }


}

module.exports = twitch_API;

