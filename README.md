# Matching job
Retourne tous les applicants qui matchent à un Job

#### 2 facons d'executer le matching : 

- Via l'url API GateWay :
https://68b7es7rv6.execute-api.eu-west-1.amazonaws.com/matching/job

- Via invoke lambda de boto3 :
invoke la fonction matching-Job 

## Doc

### Search 
permet de recuperer les infos des candidats qui matchent à une offre, rangé par page.

Parametres : 
- job : correspond à l'id du job (obligatoire)
- page : numero de la page pour pagination (facultative : default 1 )
- size : nombre d'element retourné par page (facultative : default 20 )

Return syntaxe
```
{
  "prev" : int,
  "next: int,
  "total" : int,
  "pages": int,
  "results": [
      {
          "matching" : int,
          "id" : int",
          ..
      },
      ..
  ]
}
```

Response Structure
 - prev : indique le numero de page precedent (Si page de page precedent, prev sera null)
 - next : indique le numero de page suivant (pareil, si pas de page suivant, next = null )
 - total : Nombre total d'enregistrement en tout (pas uniquement celle de la page concerné)
 - pages : indique le nombre de pages
 - results : Array avec le resultat 

 
 
### Scroll
Permet de retourner en une fois tous uniqment l'ID et le score de matching  des candidats
 
Parametres : 
- job (obligatoire)
- scroll : True (obligatoire)
- scroll_id : Si trop de resultat, permet de lancer une autre requete en reprenant la ou la précédante s'est arreté (facultative)

Return syntaxe
```
{
    "max_score" : float,
    "results": [
        {
            "_id" : int,
            "_score" : float 
        }, 
      ..
    ],
    "scroll" : string
}
```

Response Structure
- max_score : indique le score maximum qui a été retourné par ES
- results :
   - _id : l'id du candidat
   - _score : son score de pertinance par rapport à max_score
- scroll : string qui indique ou la requete s'est stopé si trop de resultat on été retourné. Il suffit de rappeler le script en envoyant scroll_id dans le json pour continuer la requete.
si scroll est vide, cela signifique que tous les résultats ont été retourné. 



