const dotenv = require('dotenv');
const Telegraf = require('telegraf');
const DocumentDAO = require('./DocumentDAO');
const GraphDAO = require('./GraphDAO');
const TwitchAPI = require('./twitch_API');

dotenv.config();

const bot = new Telegraf(process.env.BOT_TOKEN);
const graphDAO = new GraphDAO();
const documentDAO = new DocumentDAO();
const twitch = new TwitchAPI();

function makeid(length) {
  var result           = '';
  var characters       = '0123456789';
  var charactersLength = characters.length;
  for ( var i = 0; i < length; i++ ) {
    result += characters.charAt(Math.floor(Math.random() * charactersLength));
  }
  return result;
}

function stripMargin(template, ...expressions) {
  const result = template.reduce((accumulator, part, i) => {
    return accumulator + expressions[i - 1] + part;
  });
  return result.replace(/(\n|\r|\r\n)\s*\|/g, '$1');
}

function buildLikeKeyboard(gameId, currentLike) {
  return {
    inline_keyboard: [
      [1, 2, 3, 4, 5].map((v) => ({
        text: currentLike && currentLike.rank === v ? "★".repeat(v) : "☆".repeat(v),
        callback_data: v + '__' + gameId, // payload that will be retrieved when button is pressed
      })),
    ],
  }
}

// User is using the inline query mode on the bot
bot.on('inline_query', (ctx) => {
  const query = ctx.inlineQuery;
  if (query) {
    documentDAO.getGames(query.query).then((games) => {

      const answer = games.map((game) => ({
        id: game.basename,
        type: 'article',
        title: game.name,
        description: game.description,
        reply_markup: buildLikeKeyboard(game.basename),
        input_message_content: {
          message_text: stripMargin`
            |Title: ${game.name}
            |Year: ${game.year}
            |Platforms : ${game.platforms}
            |Genres: ${game.genres}
          `
        },
      }));
      ctx.answerInlineQuery(answer).then(() => {});
    });
  }
});

// User chose a game from the list displayed in the inline query
// Used to update the keyboard and show filled stars if user already liked it
bot.on('chosen_inline_result', (ctx) => {
  if (ctx.from && ctx.chosenInlineResult) {
    graphDAO.getGameLiked(ctx.from.id, ctx.chosenInlineResult.result_id).then((liked) => {
      if (liked !== null) {
        ctx.editMessageReplyMarkup(buildLikeKeyboard(ctx.chosenInlineResult.result_id, liked));
      }
    })
  }
});

bot.on('callback_query', (ctx) => {
  if (ctx.callbackQuery && ctx.from) {
    console.log(ctx.callbackQuery.data);
    const [rank, game] = ctx.callbackQuery.data.split('__');
    const liked = {
      rank: parseInt(rank, 10),
      at: new Date()
    };
    let user = {
      id: ctx.from.id,
      first_name: 'unknown',
      last_name: 'unknown',
      language_code: 'fr',
      is_bot: false,
      username: "guest_" + makeid(10),
      ...ctx.from,
    };
  console.log("Create user : " + user);
    graphDAO.upsertGameLiked(user, game, liked).then(() => {
      console.log("Like added");
      ctx.editMessageReplyMarkup(buildLikeKeyboard(game, liked));
    });


  }
});


bot.command('help', (ctx) => {
  ctx.reply(`
A demo for the project given in the MAC course at the HEIG-VD.

A user can display a game and set a reaction to this game (like, dislike).
When asked, the bot will provide a recommendation based on the games he liked or disliked.

Use inline queries to display a game, then use the inline keyboard of the resulting message to react.
Use the command /recommendstreamer to get a personalized recommendation.
  `);
});

bot.command('start', (ctx) => {
  ctx.reply('HEIG-VD Mac project bot in javascript');
});

bot.command('recommendstreamer', (ctx) => {
  console.log("Recommend Streamers" + ctx.from.id);
  //twitch.getStreamers("Horizon Zero Dawn").then((streams) => {
  let streamDisplay = [];
  graphDAO.recommendStreamers(ctx.from.id).then(async (streamers) => {

    for await(const streamer of streamers){
     await documentDAO.getStreamerById(streamer._fields[0]).then((s) => {
        streamDisplay.push({
          id: s.id,
          url: "https://www.twitch.tv/" + s.name,
          input_message_content: {
            message_text: stripMargin`
              |User: ${s.name}
              |Url: ${"https://www.twitch.tv/" + s.name}
            `}
        });
      });
    }
    for (const current in streamDisplay) {
      ctx.reply(streamDisplay[current].input_message_content.message_text).then(() =>{});
    }
  });

});

bot.command( 'recommendgame', (ctx) => {
  console.log("Recommend Games" + ctx.from.id);

  let gameDisplay = [];
  graphDAO.recommendGames(ctx.from.id).then(async (games) => {
    for await(const game of games){
      await documentDAO.getGameById(game._fields[0]).then((g) => {
        gameDisplay.push({
            id: g.basename,
            input_message_content: {
              message_text: stripMargin`
            |Title: ${g.name}
            |Year: ${g.year}
            |Platforms : ${g.platforms}
            |Genres: ${g.genres}
            |Twitch: ${"https://www.twitch.tv/directory/game/" + encodeURI(g.name)}
          `
            }});
      });
    }
    for (const current in gameDisplay) {
      ctx.reply(gameDisplay[current].input_message_content.message_text).then(() =>{});
    }
  });
})



  /*if (!ctx.from || !ctx.from.id) {
    ctx.reply('We cannot guess who you are');
  } else {
    graphDAO.recommendActors(ctx.from.id).then((records) => {
      if (records.length === 0) ctx.reply("You haven't liked enough games to have recommendations");
      else {
        const actorsList = records.map((record) => {
          const name = record.get('a').properties.name;
          const count = record.get('count(*)').toInt();
          return `${name} (${count})`;
        }).join("\n\t");
        ctx.reply(`Based your like and dislike we recommend the following actor(s):\n\t${actorsList}`);
      }
    });
  }*/



// Initialize mongo connexion
// before starting bot
documentDAO.init().then(() => {
  bot.startPolling();
});
