import dlt
import requests


@dlt.source
def instagram_source(metrics, since=None, until=None, api_secret_key=dlt.secrets.value):
    return instagram_resource(metrics, since=None, until=None, api_secret_key=api_secret_key)


def _create_auth_headers(api_secret_key):
    """Constructs Bearer type authorization header which is the most common authorization method"""
    headers = {
        "Authorization": f"Bearer {api_secret_key}"
    }
    return headers

def _paginated_get(url, headers, params, max_pages=5):
    """Requests and yields up to `max_pages` pages of results as per Twitter API docs: https://developer.twitter.com/en/docs/twitter-api/pagination"""
    while True:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        page = response.json()
        # show the pagination info
        meta = page["paging"]
        print(meta)

        yield page

        # get next page token
        next_token = meta.get('next')
        max_pages -= 1

        # if no more pages or we are at the maximum
        if not next_token or max_pages == 0:
            break
        else:
            # set the next_token parameter to get next page
            params['pagination_token'] = next_token



@dlt.resource(write_disposition="append")
def instagram_resource(metrics, since=None, until=None, api_secret_key=dlt.secrets.value):
    headers = _create_auth_headers(api_secret_key)
    #https://developers.facebook.com/docs/marketing-api/insights/
    url = 'https://graph.facebook.com/v15.0/716973036712257/insights?'
    

    # get api results for each metric
    for metric in metrics:
        params = {
            'access_token': api_secret_key,
            'period': 'day',
            'metric': metric,
            'max_results': 20,  # maximum elements per page: we set it to low value to demonstrate the paginator
            'since': since,  # '2022-11-08T00:00:00.000Z',
            'until': until,  # '2022-11-09T00:00:00.000Z',
        }

        # make an api call here
        response = _paginated_get(url, headers=headers, params=params)
        for page in response:
            yield page



if __name__=='__main__':

    metrics = ['impressions,reach,profile_views']
    dataset_name ='instagram_insights'

    # search specific dates of posts
    from datetime import datetime, timedelta, timezone

    data_interval_start = datetime.now(timezone.utc)- timedelta(days=60)
    data_interval_end = datetime.now(timezone.utc)- timedelta(days=2)
		
		# format to instagram spec -> date not datetime + since + until
    since = data_interval_start.isoformat()
    until = data_interval_end.isoformat()



    data = list(instagram_resource(metrics=metrics, since=since, until=until))

    print(data)
 
    #exit()
    pipeline = dlt.pipeline(pipeline_name='instagram', destination='bigquery', dataset_name='instagram_data')

    # run the pipeline with your parameters
    load_info = pipeline.run(instagram_source(metrics, since=None, until=None))

    # pretty print the information on data that was loaded
    print(load_info)
    print(f'data interval: {data_interval_start} to {data_interval_end}')