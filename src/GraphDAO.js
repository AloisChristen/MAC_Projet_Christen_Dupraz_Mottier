
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


  /*
  Calcul du score : Pour chaque jeu aimé (RANK > 3) par l'utilisateur, et pour chaque clip que le streamer
  possède de ce jeux, on donne RANK point.
  On ajoute également des points bonus (5) pour chaque jeu différent auquel le streamer a joué.
  (Pour favoriser les streamers qui ont beaucoup de jeux en commun avec l'utilisateur)
  On donne également des points bonus pour les jeux appartenant au genre que l'utilisateur à liker.
  (CASE WHEN l2 IS NOT NULL THEN l2.rank ELSE 0 END)
   */
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

  /*
  Calcul du score : Pour chaque jeux liké par l'utilisateur appartenant au même genre, on donne RANK points.
  On rajoute 10 points bonus si le jeu est sur une platforme possédée par l'utilisateur
  On rajoute des points si le jeux appartient à un genre aimé par l'utilisateur
   */
  recommendGames(userId) {
    return this.run(`
      match (u:User{id: $userId})-[l:LIKED]->(g:Game)-[:BELONGS_TO]->(t:Genre)<-[:BELONGS_TO]-(g2:Game)
      OPTIONAL match (u)-[:OWNS]->(p:Platform)<-[:PLAYED_ON]-(g2)
      OPTIONAL match (u)-[l2:LIKED]->(t)
      where id(g) < id(g2) and l.rank > 3
      and not ((u)-[:LIKED]->(g2))
      return g2.id, g2.basename, 
      sum(l.rank + CASE WHEN p IS NOT NULL THEN 10 ELSE 0 END + CASE WHEN l2 IS NOT NULL THEN l2.rank ELSE 0 END) as score
      order by score desc
      limit 5
    `, {
      userId: userId
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
