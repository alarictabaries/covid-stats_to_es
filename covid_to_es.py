import requests
from datetime import date, timedelta
from elasticsearch import Elasticsearch, helpers


# On va récupérer les informations Covid depuis le $from_date
from_date = date(2021, 12, 1)
# Jusqu'à aujourd'hui (ou presque...)
current_date = date.today()

# On se connecte à l'instance ElasticSearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])


# On boucle sur chaque jour depuis $from_date jusqu'à la veille d'aujourd'hui
# (compliqué d'avoir des données pour un jour qui n'est pas fini ?)
for i in range((current_date - from_date).days):

    # On utilise l'itération $i pour créer la date qui nous intéresse
    scope_date = from_date + timedelta(days=i)

    print("Requesting : " + str(scope_date))

    # On requête une API qui permet de récupérer des données COVID, par département, pour un jour précis, au format JSON
    # https://github.com/florianzemma/CoronavirusAPI-France
    covid_data = requests.get('https://coronavirusapifr.herokuapp.com/'
                              'data/departements-by-date/' + str(scope_date.day) + '-' + str(scope_date.month) + '-2021').json()

    # Petit hack car les départements 2A, 2B ne sont pas des nombres
    for row in covid_data:
        print(row)
        row['dep'] = str(row['dep'])

    print("--- Requested data : ", end="")
    print(covid_data)

    # On insère les données d'une journée dans l'instance ES, sur l'index "covid"
    result = helpers.bulk(
        es,
        covid_data,
        index="covid",
    )

    print("--- ES inserted : ", end="")
    print(result)
    # Et on boucle

print("Terminated")

