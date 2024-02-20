import pandas as pd


def rellenar():
    df = pd.read_csv('__borar_rapido/data_zendesk.csv')
    df['assignee_group_per_ticketsecond_FILLED'] = df.assignee_group_per_ticketsecond.ffill()
    df['assignee_user_per_ticketsecond_FILLED'] = df.assignee_user_per_ticketsecond.ffill()
    
    df.to_csv('__borar_rapido/filled.csv', index=False)

if __name__=='__main__':
    rellenar()