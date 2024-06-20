import warnings; warnings.filterwarnings('ignore')
import tableauserverclient as TSC
import pandas as pd
import numpy as np
import os

def _run_graphql(server,this_path):
    q = open(f'{this_path}/graph_query.graphql','r').read()
    data = server.metadata.query(q)
    print(data)
    return
    fields = data['data']['workbooks'][0]['embeddedDS'][0]['fields']
    
    d = fields[1]
    print(d)
    print(type(d))
    print(d.keys())

    
def main():
    tableau_auth = TSC.PersonalAccessTokenAuth(
        'mi_token',
        '0Rq/p74sRYObZWjTsC6HLQ==:fLBFsiHpfjjiMXPSlDC8jJ1mVa1WHeVU', #<---- aqui va el secreto
        'globalizationpartners'
    )
    server = TSC.Server('https://us-east-1.online.tableau.com', use_server_version=True)
    server.add_http_options({'verify': False})
    server.auth.sign_in(tableau_auth)
    server.version = '3.5'

    this_path = '/'.join(__file__.split('/')[:-1])
    _run_graphql(server,this_path)


if __name__=='__main__':
    main()