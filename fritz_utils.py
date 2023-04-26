import requests
import os
import numpy as np


def api(method, endpoint, data=None):
    fritz_token = os.getenv("FRITZ_TOKEN")
    if fritz_token is None:
        err = "Please specify fritz token using export FRITZ_TOKEN=<>"
        print(err)
        raise ValueError(err)
    headers = {'Authorization': f'token {fritz_token}'}
    response = requests.request(method, endpoint, params=data, headers=headers)
    return response


def query_sources_fritz(prog_id, start_date, end_date, arx=False, **kwargs):
    if arx:
        data = {
            "group_ids": prog_id,
            "startDate": start_date,
            "endDate": end_date
        }

    else:
        data = {
            "group_ids": prog_id,
            "savedAfter": start_date,
            "savedBefore": end_date,
            **kwargs
        }

    print('Querying sources in program %s, from time %s to %s' % (
    prog_id, start_date, end_date))
    response = api('GET', 'https://fritz.science/api/sources/', data=data)

    if response.status_code == 200:
        print('Success')
        return response.json()['data']['sources']

    else:
        print('Error: %s' % (response.status_code))
        return []


def query_candidates_fritz(startdate: str = '2022-10-01',
                           enddate: str = '2022-10-02',
                           groupids: str = '43'):
    data = {
        'savedStatus': 'all',
        'startDate': f'{startdate}',
        'endDate': f'{enddate}',
        'groupIDs': f'{groupids}',
        'numPerPage': 50
    }

    pagenum = 1
    query_finished = False
    candidate_list = []
    while not query_finished:
        data['pageNumber'] = pagenum
        response = api('GET', 'https://fritz.science/api/candidates', data=data)
        print(f'Queried page {pagenum}')
        pagenum += 1
        if response.status_code == 400:
            query_finished = True
            continue
        candidate_list += response.json()['data']['candidates']

    return np.array(candidate_list)