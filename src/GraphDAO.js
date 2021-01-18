
const neo4j = require('neo4j-driver');

class GraphDAO {
  
  constructor() {
    this.driver = neo4j.driver(`bolt://${process.env.GRAPHDB_HOST}`, neo4j.auth.basic('neo4j', process.env.GRAPHDB_PASSWORD));
  }

  async prepare() {
    await this.run("CREATE CONSTRAINT ON (n:Game) ASSERT n.id IS UNIQUE", {});
    await  this.run("CREATE CONSTRAINT ON (u:User) ASSERT u.id IS UNIQUE", {});
  }

  close() {
    return this.driver.close();
  }

  upsertGameLiked(user, gameName, liked) {
    return this.run(`
      MATCH (m:Game {basename: $gameName})
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
      gameName: gameName,
      userId: this.toInt(user.id),
      isBot: user.is_bot,
      firstName: user.first_name,
      lastName: user.last_name,
      languageCode: user.language_code,
      username: user.username,
      likedRank: this.toInt(liked.rank),
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

  getGenresByGameId(gameId){
    return this.run(`MATCH(:Game{id: $gameId})-[l:BELONGS_TO]->()
    `);
  }

  upsertGame(gameId, basename) {
    return this.run(
        `MERGE (m:Game{id: $gameId})
                ON CREATE SET m.basename = $gameTitle 
              RETURN m`,
        {
      gameId,
      gameTitle : basename,
    });
  }

  upsertRelationGameStreamer(streamerId, gameId){
    return this.run(`
      MATCH (g:Game{ id: $gameId })
      MATCH (s:Streamer{id: $streamerId})
      MERGE (s)-[r:PLAYS_TO]->(g)
      `, {
      gameId,
      streamerId
    });
  }

  upsertPlatform(gameId, platform) {
    return this.run(`
      MATCH (m:Game{ id: $gameId })
      MERGE (a:Platform{id: $platformId})
        ON CREATE SET a.name = $platformName
      MERGE (a)<-[r:PLAYED_ON]-(m)
    `, {
      gameId,
      platformId: platform.id,
      platformName: platform.name,
    });
  }

  upsertStreamer(gameName, streamer, count) {
    return this.run(`
      MATCH (g:Game{ basename: $gameName })
      MERGE (s:Streamer{id: $streamerId})
        ON CREATE SET s.name = $streamerName
      MERGE (s)-[r:PLAYS_TO]->(g)
        ON CREATE SET r.count = $rCount
    `, {
      gameName,
      streamerId: streamer.id,
      streamerName: streamer.name,
      rCount: count
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

  upsertPlatformLiked(user, platformName, liked) {
    console.log(user.username);
    console.log(platformName);
    return this.run(`
      MATCH (a:Platform{ name: $platform })
      MATCH (u:User{ id: $userId })
      MERGE (u)-[r:OWNS]->(a)
      ON CREATE SET r.at = $at,
                    r.rank = $rank
      ON MATCH SET  r.at = $at,
                    r.rank = $rank
    `, {
      userId: this.toInt(user.id),
      platform: platformName,
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

  recommendStreamers(userId) {
    return this.run(`
      match (u:User{id: $userId})-[l:LIKED]->(g:Game)<-[p:PLAYS_TO]-(s:Streamer)
      OPTIONAL match (u)-[l2:LIKED]->(t:Genre)<-[:BELONGS_TO]-(g2:Game)<-[p2:PLAYS_TO]-(s)
      where l.rank > 3
        AND not ((u)-[:LIKED]->(g2))
      return s.id, s.name, 
      sum(toInteger(p.count) * l.rank + 5 + CASE WHEN l2 IS NOT NULL THEN l2.rank ELSE 0 END ) as score
      ORDER BY score DESC
      limit 5
    `, {
      userId
    }).then((result) => result.records);
  }

  recommendGames(userId) {
    return this.run(`
      match (u:User{id: userId})-[l:LIKED]->(g:Game)-[:BELONGS_TO]->(t:Genre)<-[:BELONGS_TO]-(g2:Game)
      OPTIONAL match (u)-[:OWNS]->(p:Platform)<-[:PLAYED_ON]-(g2)
      OPTIONAL match (u)-[l2:LIKED]->(t)
      where id(g) < id(g2) and l.rank > 3
      and not ((u)-[:LIKED]->(g2))
      return g2.id, g2.basename, 
      sum(l.rank + CASE WHEN p IS NOT NULL THEN 10 ELSE 0 END + CASE WHEN l2 IS NOT NULL THEN l2.rank ELSE 0 END) as score
      order by score desc
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
