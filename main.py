import warnings; warnings.filterwarnings('ignore') #<-- default de python
import tableauserverclient as TSC #<--- este es el unico por descargar
import pandas as pd

def _get_customquery_tables(q):
    q = q.lower().replace('\n',' ')
    ls = []
    for con in ['from ','join ']:
        l = q.split(con)[1:]
        l = [s.split(' ')[0] for s in l]
        ls.append(l)

    ls = [d for l in ls for d in l]
    ls = [s for s in ls if '.' in s]
    return list(set(ls))

def get_tables_from_custqueries():
    df = pd.read_csv('02_customqueries_tables.csv')
    df['customquery_datasources'] = df.customQuery.apply(lambda s: _get_customquery_tables(str(s)))
    print(df)
    data = [[
        {
            'workbookID':row.workbookID,
            # 'custQuery':row.customQuery,
            'table':t
        }  for t in _get_customquery_tables(str(row.customQuery))
    ] for _,row in df.iterrows()]

    data = pd.DataFrame([e for d in data for e in d])
    return data

def get_customquery_previous(server,df_wbs):
    def _try_getting_data(l,f1,f2):
        try:
            return l[f1][f2]
        except:
            return None
    
    df_concat = pd.DataFrame()
    for i,id in enumerate(df_wbs.id):
        print(f'\tloading {i+1}/{df_wbs.shape[0]} ({(i+1)/df_wbs.shape[0]:.1%}) | {df_wbs[df_wbs.id==id].id.iloc[0]} | {df_wbs[df_wbs.id==id].name.iloc[0]}')
        q = open('_custom_query.graphql','r').read().replace(r'${id}',id)
        data = server.metadata.query(q)
        data = data['data']['workbooks']

        # NESTED DATA
        print(pd.DataFrame(data[0]['embeddedDS'][0]))
        # pd.DataFrame(data[0]['embeddedDS'][0]).to_csv('_borrar.csv')
        data = [
            [
                [
                    [
                        {
                            'workbookID':d['id'],
                            'datasource':e['datasourceName'],
                            'connType':_try_getting_data(l,'table','connectionType'),
                            'customQuery':_try_getting_data(l,'table','query')
                        } for l in s['columns']
                    ] for s in e['fields'] if 'columns' in s.keys()
                ] for e in d['embeddedDS']
            ] for d in data
        ]

        # UNNESTED DATA
        df = pd.DataFrame([dic for d in data for e in d for s in e for dic in s]).drop_duplicates().reset_index(drop=True)

        df_concat = pd.concat([df, df_concat])
        # print(df.head(3))

    
    return df_concat

def get_workbook_owners(server,df_wbs):
    df = pd.DataFrame()
    for i,row in df_wbs.iterrows():
        try:
            data = server.metadata.query(open('_workbooks_owners.graphql','r').read().replace(r'${id}',row.id))
            data = pd.DataFrame(data['data']['workbooks'])
            df = pd.concat([df, data])
            print(f'{i+1}/{df_wbs.shape[0]} ({(i+1)/df_wbs.shape[0]:.1%}) | wb_id:',row.id)
        except Exception as err:
            print('failed: ',row.id)
            print(err)
            continue

    df_wbs = df_wbs.merge(df, on='id', how='left')
    return df_wbs

def save_thumbnails(server,df):
    for i,row in df.iterrows():
        print(i,row.luid,f'{(i+1)/df.shape[0]:.1%}')
        workbooks = server.workbooks
        workbook = workbooks.get_by_id(row.luid)
        workbooks.populate_preview_image(workbook) #<-- obligatorio
        
        open(f'thumbnails/{row.luid}.png', 'wb').write(workbook.preview_image)

def get_workbook_ids(server):
    graphQL = open('_workbooks_ids.graphql','r').read()
    data = server.metadata.query(graphQL)
    data = pd.DataFrame(data['data']['workbooks'])
    data['url']     = 'https://us-east-1.online.tableau.com/#/site/globalizationpartners/workbooks/'+data.vizportalUrlId+'/views'
    data['img_url'] = 'https://raw.githubusercontent.com/saulalvarezGP/landing_page/main/thumbnails/'+data.luid+'.png'

    return data

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

    df_wbs = get_workbook_ids(server)
    
    # save_thumbnails(server,df_wbs)
    df_wbs.to_csv('01_workbooks.csv', index=False)
    return
    
    df_wbs = get_workbook_owners(server, df_wbs)
    df_wbs.to_csv('01_workbooks_with_owners.csv', index=False)
    
    df_ds  = get_customquery_previous(server, df_wbs)
    df_ds.to_csv('02_customqueries_tables.csv', index=False)
    
    df_tables = get_tables_from_custqueries()
    df_tables.to_csv('03_full_table_extraction.csv', index=False)


if __name__=='__main__':
    main()