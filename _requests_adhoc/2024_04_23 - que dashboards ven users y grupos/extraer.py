import warnings; warnings.filterwarnings('ignore')
import tableauserverclient as TSC
import pandas as pd
import numpy as np

def encontrar_fuente(campo, query):
    # EXTRAE ALIAS DE LA TABLA
    campo = campo+' '
    query = query.replace('\n',' ').replace('\t',' ')
    while ' '*2 in query: query = query.replace(' '*2,' ')
    l = query.split(campo)

    if len(l)>2: 
        print(f'hay más de una coincidencia para {campo}')
        print(len(l))
        print(l[0])
        return
    
    if l[0][-4:].lower()==' as ':
        # aqui puede existir campos compuestos por otros

        original = l[0].split(',')[-1].lower().replace(' as ','')
        print(f'original: {original} -> renombrado: {campo}')

    else:
        original = l[0].split(',')[-1]+campo
        print(f'original: {original}')

    
    
    # BUSCA ALIAS DE LA TABLA, PUEDE EXISTIR CON LOS SIGUIENTES PATRONES
    alias = original.split('.')[0]
    
    # FROM schema.table AS alias  
    if f' as {alias} ' in query.lower():
        l = query.lower().split(f' as {alias} ')
        if l[0].split(' ')[-2] in ['from','join']:
            datasource = l[0].split(' ')[-1]
            print(f'datasource: {datasource}')
            return {
                'original':original,
                'datasource':datasource
            }
        

    # elif f' {alias} ' in query.lower():
    #     # FROM schema.table alias
    #     l = query.lower().split(f' {alias} ')
    #     if l[0].split(' ')[-2] in ['from','join']:
    #         datasource = l[0].split(' ')[-1]
    #         print(f'datasource: {datasource}')
    #         return {
    #             'original':original,
    #             'datasource':datasource
    #         }

    # FROM schema.table
    # POR PROGRAMAR

    print('\n'*2)
    
def get_dashboard_sheets(server):
    q = open('___borrar.graphql','r').read()
    data = server.metadata.query(q)
    # d = data['data']['workbooks'][0]['embeddedDatasources']
    # print(d)
    # print(pd.DataFrame(d))
    
    data = data['data']['workbooks'][0]['sheets']
    df = pd.DataFrame(data)
    
    df['dashboardName'] = df.containedInDashboards.apply(lambda l: l[0]['name'] if len(l) > 0 else np.nan)
    df['dashboardID'] = df.containedInDashboards.apply(lambda l: l[0]['id'] if len(l) > 0 else np.nan)
    df = df[[c for c in df if c!='containedInDashboards']]

    df = df.sort_values(by='dashboardName')

    def _get_custom_query(d):
        try:
            q = np.nan if d is np.nan else d[0]['table']['query']
        except:
            q = 'con error'
        return q

    df_concat = pd.DataFrame()
    for _,row in df.iterrows():
        d = pd.DataFrame(row.instancedFields)
        d['visualization'] = row.visualization
        d['dashboardName'] = row.dashboardName
        #d['customQuery'] = d.customQueryContainer.apply(lambda d: str(_get_custom_query(d))[:300])
        #d = d.drop(columns=['customQueryContainer'],axis=1)

        df_concat = pd.concat([df_concat, d])
        
    df_concat['originalName'] = df_concat.originalName.str.replace(']','').str.replace('[','')
    df_concat['is_calculated_field'] = ~df_concat.formula.isna()
    #df_concat['datasourceName_dic'] = df_concat.apply(axis=1, func=lambda row: encontrar_fuente(row.originalName, row.customQuery) if row.customQuery is not np.nan else np.nan)
    # df_concat['schema'] = df_concat.datasourceName_dic.apply(lambda d: d['datasource'] if isinstance(d, dict) else d)
    # df_concat['datasourceName'] = df_concat.datasourceName_dic.apply(lambda d: d['original'] if isinstance(d, dict) else d)
    # df_concat['datasourceName'] = df_concat.datasourceName.astype(str).apply(lambda s: s.split('.')[1] if '.' in s else s)

    print(df_concat)
    query = df_concat.iloc[7].customQueryContainer
    print(query[0]['table']['query'])
    df_concat.to_csv('_borrar.csv', index=False)
    
def main():
    tableau_auth = TSC.PersonalAccessTokenAuth(
        'mi_token',
        'bB/kYrazTN6cTNCBAyIxaQ==:0Mo8DoDLaKevgqtFT3MOPtj2H8WZ6LC5', #<---- aqui va el secreto
        'globalizationpartners'
    )
    server = TSC.Server('https://us-east-1.online.tableau.com', use_server_version=True)
    server.auth.sign_in(tableau_auth)

    get_dashboard_sheets(server)


if __name__=='__main__':
    main()