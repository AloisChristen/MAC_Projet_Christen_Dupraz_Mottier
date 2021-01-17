data :

    - Check les imports :
		- insertions des streamer dans MongoDB
			- insertions des streamer_id dans Neo4j
			- insertions des liens streamer [a joué à]-> jeux dans Neo4j (avec entrées aléatoires)
start:    

	- commandes :
        - /recommandGame: propose des jeux en fonction des jeux likés
		- /recommandStreamer: propose des streamer en fonction des jeux likés

	- inline : 
		-> recherche et sélection de jeux par mot-clef
		-> like du jeu possible
		-> propositions de stream sur le jeu choisit
		-> like du streamer | lien vers le stream
			
nettoyage:

	- refactorisation de loadData
		