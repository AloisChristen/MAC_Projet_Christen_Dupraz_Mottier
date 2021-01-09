import * as dotenv from 'dotenv';

dotenv.config();

import { Telegraf } from 'telegraf';
import { InlineKeyboardMarkup, InlineQueryResultArticle } from 'telegraf/typings/telegram-types';

import DocumentDAO from './DocumentDAO';
import GraphDAO from './GraphDAO';
import { Liked, likedValues } from './Model';

const bot = new Telegraf(process.env.BOT_TOKEN);
const graphDAO = new GraphDAO();
const documentDAO = new DocumentDAO();

function stripMargin(template: TemplateStringsArray, ...expressions: any[]) {
  const result = template.reduce((accumulator, part, i) => {
      return accumulator + expressions[i - 1] + part;
  });
  return result.replace(/(\n|\r|\r\n)\s*\|/g, '$1');
}

function buildLikeKeyboard(movieId: string, currentLike?: Liked): InlineKeyboardMarkup {
  return {
    inline_keyboard: [
      likedValues.map((v) => ({
        text: currentLike && currentLike.rank === v ? "★".repeat(v) : "☆".repeat(v),
        callback_data: v + '__' + movieId, // payload that will be retrieved when button is pressed
      })),
    ],
  }
}

// User is using the inline query mode on the bot
bot.on('inline_query', async (ctx) => {
  const query = ctx.inlineQuery;
  if (query) {
    const movies = await documentDAO.getMovies(query.query);
    const answer: InlineQueryResultArticle[] = movies.map((movie) => ({
      id: movie._id,
      type: 'article',
      title: movie.title,
      description: movie.description,
      reply_markup: buildLikeKeyboard(movie._id),
      input_message_content: {
        message_text: stripMargin`
          |Title: ${movie.title}
          |Description: ${movie.description},
          |Year: ${movie.year}
          |Actors: ${movie.actors}
          |Genres: ${movie.genre}
        `
      },
    }));
    ctx.answerInlineQuery(answer);
  }
});

// User chose a movie from the list displayed in the inline query
// Used to update the keyboard and show filled stars if user already liked it
bot.on('chosen_inline_result', async (ctx) => {
  if (ctx.from && ctx.chosenInlineResult) {
    const liked = await graphDAO.getMovieLiked(ctx.from.id, ctx.chosenInlineResult.result_id);
    if (liked !== null) {
      ctx.editMessageReplyMarkup(buildLikeKeyboard(ctx.chosenInlineResult.result_id, liked));
    }
  }
});

bot.on('callback_query', async (ctx) => {
  if (ctx.callbackQuery && ctx.from) {
    const [rank, movieId] = ctx.callbackQuery.data.split('__');
    console.log(rank, movieId);
    const liked: Liked = {
      rank: parseInt(rank, 10),
      at: new Date()
    };
    await graphDAO.upsertMovieLiked({
      first_name: 'unknown',
      last_name: 'unknown',
      language_code: 'fr',
      is_bot: false,
      username: 'unknown',
      ...ctx.from,
    }, movieId, liked);
    ctx.editMessageReplyMarkup(buildLikeKeyboard(movieId, liked));
  }
});


bot.command('help', (ctx) => {
  ctx.reply(`
A demo for the project given in the MAC course at the HEIG-VD.

A user can display a movie and set a reaction to this movie (like, dislike).
When asked, the bot will provide a recommendation based on the movies he liked or disliked.

Use inline queries to display a movie, then use the inline keyboard of the resulting message to react.
Use the command /recommendactor to get a personalized recommendation.
  `);
});

bot.command('start', (ctx) => {
  ctx.reply('HEIG-VD Mac project example bot in javascript');
});

bot.command('recommendactor', (ctx) => {
  if (!ctx.from || !ctx.from.id) {
    ctx.reply('We cannot guess who you are');
  } else {
    graphDAO.recommendActors(ctx.from.id).then((records) => {
      if (records.length === 0) ctx.reply("You haven't liked enough movies to have recommendations");
      else {
        const actorsList = records.map((record) => {
          const name = record.get('a').properties.name;
          const count = record.get('count(*)').toInt();
          return `${name} (${count})`;
        }).join("\n\t");
        ctx.reply(`Based your like and dislike we recommend the following actor(s):\n\t${actorsList}`);
      }
    });
  }
});


// Initialize mongo connexion
// before starting bot
documentDAO.init().then(() => {
  bot.startPolling();
});