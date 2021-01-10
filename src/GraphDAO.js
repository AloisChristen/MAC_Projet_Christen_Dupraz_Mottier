
const neo4j = require('neo4j-driver');

class GraphDAO {
  
  constructor() {
    this.driver = neo4j.driver(`bolt://${process.env.GRAPHDB_HOST}`, neo4j.auth.basic('neo4j', process.env.GRAPHDB_PASSWORD));
  }

  prepare() {
    return new Promise((resolve) => {
      this.run("CREATE CONSTRAINT ON (n:Game) ASSERT n.id IS UNIQUE", {}).then(() => {
        this.run("CREATE CONSTRAINT ON (u:User) ASSERT u.id IS UNIQUE", {}).then(() => resolve());
      });
    });
  }

  close() {
    return this.driver.close();
  }

  upsertGameLiked(user, gameId, liked) {
    return this.run(`
      MATCH (m:Game {id: $gameId})
        MERGE (u:User {id: $userId})
          ON CREATE SET u.isBot = $isBot,
                        u.firstName = $firstName,
                        u.lastName = $lastName,
                        u.username = $username,
                        u.languageCode = $languageCode
          ON MATCH SET  u.isBot = $isBot,
                        u.firstName = $firstName,
                        u.lastName = $lastName,
                        u.username = $username,
                        u.languageCode = $languageCode
        MERGE (u)-[l:LIKED]->(m)
          ON CREATE SET l.rank = $likedRank,
                        l.at = $likedAt
          ON MATCH SET  l.rank = $likedRank,
                        l.at = $likedAt
    `, {
      gameId,
      isBot: user.is_bot,
      firstName: user.first_name,
      lastName: user.last_name,
      languageCode: user.language_code,
      username: user.username,
      userId: this.toInt(user.id),
      likedRank: liked.rank,
      likedAt: this.toDate(liked.at),
    });
  }

  getGameLiked(userId, gameId) {
    return this.run('MATCH (:User{id: $userId})-[l:LIKED]-(:Game{id: $gameId}) RETURN l', {
      userId,
      gameId,
    }).then((res) => {
      if (res.records.length === 0) return null;
      else {
        const record = res.records[0].get('l');
        return {
          rank: record.properties.rank,
          at: record.properties.at,
        }
      }
    });
  }

  upsertGame(gameId, gameTitle) {
    return this.run('MERGE (m:Game{id: $gameId}) ON CREATE SET m.basename = $gameTitle RETURN m', {
      gameId,
      gameTitle,
    })
  }

  upsertPlatform(gameId, actor) {
    return this.run(`
      MATCH (m:Game{ id: $gameId })
      MERGE (a:Actor{id: $actorId})
        ON CREATE SET a.name = $actorName
      MERGE (a)-[r:PLAYED_IN]->(m)
    `, {
      gameId,
      actorId: actor.id,
      actorName: actor.name,
    });
  }

  upsertGenre(gameId, genre) {
    return this.run(`
      MATCH (m:Game{ id: $gameId })
      MERGE (g:Genre{id: $genreId})
        ON CREATE SET g.name = $genreName
      MERGE (m)-[r:BELONGS_TO]->(g)
    `, {
      gameId,
      genreId: genre.id,
      genreName: genre.name,
    });
  }

  upsertUser(user) {
    return this.run(`
      MERGE (u:User {id: $userId})
      ON CREATE SET u.isBot = $isBot,
                    u.firstName = $firstName,
                    u.lastName = $lastName,
                    u.username = $username,
                    u.languageCode = $languageCode
      ON MATCH SET  u.isBot = $isBot,
                    u.firstName = $firstName,
                    u.lastName = $lastName,
                    u.username = $username,
                    u.languageCode = $languageCode
    `, {
      userId: this.toInt(user.id),
      firstName: user.first_name,
      lastName: user.last_name,
      username: user.username,
      languageCode: user.language_code,
      isBot: user.is_bot,
    });
  }

  upsertAdded(userId, gameId, added) {
    return this.run(`
      MATCH (m:Game{ id: $gameId })
      MATCH (u:User{ id: $userId })
      MERGE (u)-[r:ADDED]->(m)
        ON CREATE SET r.at = $at
        ON MATCH SET  r.at = $at
    `, {
      userId: this.toInt(userId),
      gameId,
      at: this.toDate(added.at),
    });
  }

  upsertGameUserLiked(userId, gameId, liked) {
    return this.run(`
      MATCH (m:Game{ id: $gameId })
      MATCH (u:User{ id: $userId })
      MERGE (u)-[r:LIKED]->(m)
        ON CREATE SET r.at = $at,
                      r.rank = $rank
        ON MATCH SET  r.at = $at,
                      r.rank = $rank
    `, {
      userId: this.toInt(userId),
      gameId,
      at: this.toDate(liked.at),
      rank: this.toInt(liked.rank)
    });
  }

   upsertGenreLiked(userId, genreId, liked) {
    return this.run(`
      MATCH (g:Genre{ id: $genreId })
      MATCH (u:User{ id: $userId })
      MERGE (u)-[r:LIKED]->(g)
      ON CREATE SET r.at = $at,
                    r.rank = $rank
      ON MATCH SET  r.at = $at,
                    r.rank = $rank
    `, {
      userId: this.toInt(userId),
      genreId: this.toInt(genreId),
      at: this.toDate(liked.at),
      rank: liked.rank
    });
  }

  upsertActorLiked(userId, actorId, liked) {
    return this.run(`
      MATCH (a:Actor{ id: $actorId })
      MATCH (u:User{ id: $userId })
      MERGE (u)-[r:LIKED]->(g)
      ON CREATE SET r.at = $at,
                    r.rank = $rank
      ON MATCH SET  r.at = $at,
                    r.rank = $rank
    `, {
      userId: this.toInt(userId),
      actorId: this.toInt(actorId),
      at: this.toDate(liked.at),
      rank: this.toInt(liked.rank)
    });
  }

  upsertRequested(userId, gameId, requested) {
    return this.run(`
      MATCH (m:Game{ id: $gameId })
      MATCH (u:User{ id: $userId })
      MERGE (u)-[r:REQUESTED]->(m)
        ON CREATE SET r.at = $at
        ON MATCH SET  r.at = $at
    `, {
      userId: this.toInt(userId),
      gameId,
      at: this.toDate(requested.at),
    });
  }

  upsertCommentAboutGame(userId, gameId, comment) {
    return this.run(`
      MATCH (m:Game{ id: $gameId })
      MATCH (u:User{ id: $userId })
      MERGE (c:Comment{ id: $commentId })
        ON CREATE SET c.text = $commentText,
                      c.at = $commentAt
        ON MATCH SET  c.text = $commentText,
                      c.at = $commentAt
      MERGE (u)-[r:WROTE]->(c)
      MERGE (c)-[r:ABOUT]->(m)
    `, {
      userId: this.toInt(userId),
      gameId,
      commentId: this.toInt(comment.id),
      commentAt: this.toDate(comment.at),
      commentText: comment.text
    });
  }

  upsertCommentAbountComment(userId, commentId, comment) {
    return this.run(`
      MATCH (cc:Comment{ id: $commentId })
      MATCH (u:User{ id: $userId })
      MERGE (c:Comment{ id: $subCommentId })
        ON CREATE SET c.text = $subCommentText,
                      c.at = $subCommentAt
        ON MATCH SET  c.text = $subCommentText,
                      c.at = $subCommentAt
      MERGE (u)-[r:WROTE]->(c)
      MERGE (c)-[r:ABOUT]->(cc)
    `, {
      userId: this.toInt(userId),
      commentId: this.toInt(commentId),
      subCommentId: this.toInt(comment.id),
      subCommentAt: this.toDate(comment.at),
      subCommentText: comment.text
    });
  }

  recommendActors(userId) {
    /*
    return this.run(`
      match (u:User{id: $userId})-[l:LIKED]->(m:Game)<-[:PLAYED_IN]-(a:Actor)-[:PLAYED_IN]->(m2:Game)<-[l2:LIKED]-(u)
      where id(m) < id(m2) and l.rank > 3 and l2.rank > 3
      return a, count(*)
      order by count(*) desc
      limit 5
    `, {
      userId
    }).then((result) => result.records);
    */
   return this.run(`
      match (u:User{id: $userId})-[l:LIKED]->(m:Game)<-[:PLAYED_IN]-(a:Actor)
      return a, count(*)
      order by count(*) desc
      limit 5
    `, {
      userId
    }).then((result) => result.records);
  }

  toDate(value) {
    return neo4j.types.DateTime.fromStandardDate(value);
  }

  toInt(value) {
    return neo4j.int(value);
  }

  run(query, params) {
    const session = this.driver.session();
    return new Promise((resolve) => {
      session.run(query, params).then((result) => {
        session.close().then(() => resolve(result));
      });
    });
  }
}

module.exports = GraphDAO;
