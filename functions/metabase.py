import requests
import pandas as pd
import datetime as dt

def make_metabase_session(api_key: str) -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "Content-Type": "application/json",
        "X-API-KEY": api_key
    })
    return session

def get_metabase_cards(url: str, session: requests.Session) -> dict:
    response = session.get(f"{url}/api/card")
    if response.status_code != 200:
        print(f"Error: {response.status_code} - {response.text}")
    else:
        cards = response.json()

    return cards

def parse_metabase_cards(cards: requests.Session) -> pd.DataFrame:
    data = []
    for card in cards:
        card_id = card["id"]
        card_name = card["name"]
        db_id = card['database_id']
        created_at = card["created_at"]
        last_used_at = card['last_used_at']
        last_updated_at = card['updated_at']
        archived = card['archived'] 
        collection_id = card['collection_id']
        creator = card['creator']['common_name']
        try: 
            last_user_edit = card['last-edit-info']['email']
        except:
            last_user_edit = None
        card_sql = card.get("dataset_query", {}).get("native", {}).get("query")
        data.append(
            {
                "card_id": card_id, 
                "collection_id": collection_id,
                "card_name": card_name, 
                "card_sql": card_sql,
                "db_id": db_id,
                "created_at": created_at,
                "created_by" : creator,
                "last_updated_at": last_updated_at,
                "last_updated_by" : last_user_edit,
                "last_used_at": last_used_at,
                "archived": archived,
            }
        )

    df = pd.DataFrame(data)

    return df

def card_flagger(row):
    flag = False
    if row[['raw_hex', 'raw_fivetran', 'raw_stitch', 'raw_airbyte', 'raw_dataddo', 'raw_portable']].any():
        flag = True 
    if row['db_name_x'] != 'data_prod':
        flag = True 
    return flag 

def raw_db_finder(row):
    raw_dbs = []
    if pd.notna(row['card_sql']):  # Check if SQL is not null
        if 'raw_stitch' in row['card_sql']:
            raw_dbs.append('raw_stitch')
        if 'raw_hex' in row['card_sql']:
            raw_dbs.append('raw_hex')
        if 'raw_fivetran' in row['card_sql']:
            raw_dbs.append('raw_fivetran')
        if 'raw_airbyte' in row['card_sql']:
            raw_dbs.append('raw_airbyte')
        if 'raw_portable' in row['card_sql']:
            raw_dbs.append('raw_portable')
        if 'raw_dataddo' in row['card_sql']:
            raw_dbs.append('raw_dataddo')
    return raw_dbs

def transform_metabase_cards(df: pd.DataFrame) -> pd.DataFrame:
    databases = {
        "3" : "legacy - scorecard",
        "34" : "legacy - prod",
        "35" : "data_prod",
        "37" : "raw_hex",
        "38" : "raw_portable",
        "40" : "raw_dataddo",
        "41" : "raw_tarmac",
        "67" : "raw_fivetran",
        "68" : "raw_airbyte",
        "100" : "data_stg",
        "133" : "raw_planner"
    }
    df_without_onsite_embeddings = df[df['collection_id'] != 1950].copy()
    df_without_metadata = df_without_onsite_embeddings[df_without_onsite_embeddings['db_id'] != 13371337].copy()
    df_without_metadata['db_name'] = df_without_metadata['db_id'].apply(lambda x: databases[str(x)])
    df_without_metadata['raw_dbs_used'] = df_without_metadata.apply(raw_db_finder, axis=1)

    mask = df_without_metadata['raw_dbs_used'].apply(lambda x: len(x) > 0)
    raw_db_usage = df_without_metadata[mask].copy()

    for db in ['raw_hex', 'raw_stitch', 'raw_fivetran', 'raw_airbyte', 'raw_portable', 'raw_dataddo']:
        raw_db_usage[db] = raw_db_usage['raw_dbs_used'].apply(lambda x: db in x)

    df_with_raw_table_usage = df_without_metadata.merge(
        raw_db_usage,
        left_on= 'card_id',
        right_on= 'card_id',
        how= 'left'
    ).copy()

    df_with_raw_table_usage['flagged'] = df_with_raw_table_usage.apply(card_flagger, axis = 1)
    df_with_raw_table_usage['days_since_last_usage'] = (dt.datetime.now(dt.timezone.utc) - pd.to_datetime(df_with_raw_table_usage['last_used_at_x'])).dt.days
    df_with_raw_table_usage['days_since_last_update'] = (dt.datetime.now(dt.timezone.utc) - pd.to_datetime(df_with_raw_table_usage['last_updated_at_x'])).dt.days
    
    bins = list(range(0, df_with_raw_table_usage['days_since_last_update'].max() + 100, 100))
    labels = [f'since {i}-{i+99} days' for i in range(0, (len(bins)-1) * 100, 100)]  
    df_with_raw_table_usage['update_category'] = pd.cut(df_with_raw_table_usage['days_since_last_update'], 
                                            bins=bins,
                                            labels=labels,
                                            include_lowest=True)

    bins = list(range(0, df_with_raw_table_usage['days_since_last_usage'].max() + 10, 10))
    labels = [f'since {i}-{i+9} days' for i in range(0, (len(bins)-1) * 10, 10)]  
    df_with_raw_table_usage['usage_category'] = pd.cut(df_with_raw_table_usage['days_since_last_usage'], 
                                            bins=bins,
                                            labels=labels,
                                            include_lowest=True)
    
    return df_with_raw_table_usage


def get_cards_to_archive(df: pd.DataFrame) -> pd.DataFrame:
    
    usage_condition = df['days_since_last_usage'] > 100

    cards_to_archive = df[usage_condition]

    return cards_to_archive

def archive_card(card_id: int, session: requests.Session, url: str):   
    response = session.put(f"{url}/api/card/{card_id}",  json={'archived': True})
    return response

def archive_batch(df: pd.DataFrame, session: requests.Session, url: str) -> str:
    card_ids = df['card_id'].unique()
    archived = []
    not_archived = []

    for card_id in card_ids:
        print(f"Archiving card_id {card_id}")
        result = archive_card(card_id, session, url)
        content=str(result.content)
        was_archived = content.__contains__('"archived":true')
        if was_archived:
            print(f"Card {card_id} succesfully archived")
            archived.append(card_id)
        else:
            print(f"Card {card_id} was not archived")
            not_archived.append(card_id)
    
    output = f"{len(archived)} cards were archived. {len(not_archived)} cards were NOT archived. Cards to try again: {not_archived}"

    return output