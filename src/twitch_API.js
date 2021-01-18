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

    async getGame(game){
        return this.apiClient.helix.games.getGameByName(game.name);
    }

    resetPagination(){
        this.paginator = this.apiClient.helix.games.getTopGamesPaginated();
    }

    async getNextGames(){
        await sleep(500);
        return this.paginator.getNext();
    }

    async getAllGamesPlayed(streamer, nb = 20){
        // On passe par les derniers clips des streamers pour savoir à quels jeux ils ont joué
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
