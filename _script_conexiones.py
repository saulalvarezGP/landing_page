import warnings; warnings.filterwarnings('ignore')
import tableauserverclient as TSC
import pandas as pd

def main():
    tableau_auth = TSC.PersonalAccessTokenAuth(
        'mi_token',
        'rtV5aGbESfyu2+KhMs9Axw==:cucmPGijwevg2TAO8FwCiZdxQMq1Nqrg', #<---- aqui va el secreto
        'globalizationpartners'
    )
    server = TSC.Server('https://us-east-1.online.tableau.com', use_server_version=True)
    server.auth.sign_in(tableau_auth)

    q = open('_receta_magica_por_id.graphql','r').read()
    data = server.metadata.query(q)
    d = data['data']['workbooks'][0]['embeddedDatasources']
    print(d)
    print(pd.DataFrame(d))


if __name__=='__main__':
    main()