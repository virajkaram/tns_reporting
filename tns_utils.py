import requests
import json
from collections import OrderedDict
import numpy as np
from astropy.time import Time
from fritz_utils import api
import os
import pandas as pd

TNS = "www.wis-tns.org"
TEST_TNS_URL = "sandbox.wis-tns.org"
url_tns_api = "https://" + TNS + "/api"
url_test_tns_api = "https://" + TEST_TNS_URL + "/api"
TNS_BOT_ID = os.getenv('TNS_BOT_ID', None)
TNS_BOT_NAME = os.getenv("TNS_BOT_NAME", None)
TNS_API_KEY = os.getenv("TNS_API_KEY", None)

if TNS_BOT_NAME is None:
    raise ValueError("Please specify TNS_BOT_NAME in environment variables")
if TNS_BOT_ID is None:
    raise ValueError("Please specify TNS_BOT_ID in environment variables")
if TNS_API_KEY is None:
    raise ValueError("Please specify TNS_API_KEY in environment variables")

filtkeys = {"ztfg": "110", "ztfr": "111", "ztfi": "112"}
ztf_instrument_id = 1

allowed_instrument_ids = np.array([ztf_instrument_id])


def set_bot_tns_marker():
    tns_marker = 'tns_marker{"tns_id": ' + str(TNS_BOT_ID) + \
                 ', "type": "bot", "name": "' + TNS_BOT_NAME + '"}'
    return tns_marker


def format_to_json(source):
    """
    function for changing data to json format

    :param source:
    :return:
    """
    parsed = json.loads(source, object_pairs_hook=OrderedDict)
    result = json.dumps(parsed, indent=4)
    return result


def search_tns(ra, dec, internal_name='', radius=4, radius_units='arcsec', test=False):
    internal_name_exact_match = int(internal_name != '')
    search_obj = [("ra", ra), ("dec", dec), ("radius", radius), ("units", radius_units),
                  ("objname", ""), ("objname_exact_match", 0),
                  ("internal_name", internal_name),
                  ("internal_name_exact_match", internal_name_exact_match),
                  ("objid", ""), ("public_timestamp", "")]
    if test:
        search_url = url_test_tns_api + "/get/search"
    else:
        search_url = url_tns_api + "/get/search"
    tns_marker = set_bot_tns_marker()
    headers = {'User-Agent': tns_marker}
    json_file = OrderedDict(search_obj)
    search_data = {'api_key': TNS_API_KEY, 'data': json.dumps(json_file)}
    print(f"Searching on {search_url}")
    response = requests.post(search_url, headers=headers, data=search_data)

    return response


def check_exists_on_tns(ra, dec, ztf_name, radius=4, radius_units='arcsec', test=False):
    response = search_tns(ra, dec, ztf_name, radius, radius_units, test=test)
    json_data = response.json()
    results = json_data['data']['reply']

    source_exists_on_tns = (len(results) > 0)
    tns_names = None
    if source_exists_on_tns:
        # since we are querying with the internal name, prefix appears in
        # the result only if it already has been reported with
        # the same internal name
        tns_names = [f"{x['prefix']}{x['objname']}" for x in results]

    return source_exists_on_tns, tns_names


def send_json_report(json_report: dict, test=False):
    if test:
        json_url = url_test_tns_api + "/bulk-report"
    else:
        json_url = url_tns_api + "/bulk-report"
    tns_marker = set_bot_tns_marker()
    headers = {'User-Agent': tns_marker}
    json_data = {'api_key': TNS_API_KEY, 'data': json_report}

    print(f"Sending report {json_report} to {json_url}")
    response = requests.post(json_url, headers=headers, data=json_data)
    return response


def get_source_properties_dictionary(internal_name, ra, dec, at_type=1, remarks=None):
    response = api('GET', f"https://fritz.science/api/sources/{internal_name}",
                   data={'includePhotometry': True})
    photometry = response.json()['data']['photometry']
    photjds = np.array([p['mjd'] + 2400000.5 for p in photometry])
    photfilters = np.array([p['filter'] for p in photometry])
    photfluxes = np.array([x['flux'] for x in photometry], dtype=float)
    photfluxerrs = np.array([x['fluxerr'] for x in photometry], dtype=float)
    photzps = np.array([x['zp'] for x in photometry], dtype=float)
    # photpos = np.array([True for p in photometry])

    photfluxes = np.where(photfluxes != None, photfluxes, np.nan)
    photfluxerrs = np.where(photfluxerrs != None, photfluxerrs, np.nan)

    photmags = -2.5 * np.log10(photfluxes) + photzps
    photmagerrs = 1.086 * photfluxerrs / photfluxes
    photmaglims = -2.5 * np.log10(
        5 * photfluxerrs) + photzps  # TODO : get correct maglims
    photinstruments = np.array([p['instrument_id'] for p in photometry])

    detflags = (~(np.isnan(photmags))) & (
        [x in allowed_instrument_ids for x in photinstruments])
    if np.sum(detflags) == 0:
        print('No available public detections .. Skipping')
        return None
    discjd = np.min(photjds[detflags])
    discdateobj = Time(discjd, format='jd')
    # discdate = discdateobj.iso.split()[0] + ('%.5f' % (
    #         discdateobj.mjd - np.floor(discdateobj.mjd)))[1:]
    discdate = discjd
    discmag = photmags[photjds == discjd][0]
    discfilt = photfilters[photjds == discjd][0]
    discfiltkey = filtkeys[discfilt]
    discmagerr = photmagerrs[photjds == discjd][0]
    discmaglim = photmaglims[photjds == discjd][0]

    limflags = (photjds < discjd) & (np.isnan(photmags)) \
               & (~np.isnan(photmaglims)) & (
                   [x in allowed_instrument_ids for x in photinstruments])
    # & (photprogs == 1)
    if np.sum(limflags) == 0:
        print('RED ALERT: No available public limits .. Will use defaults ..')
        lastlimjd = 2458270
        lastlimdateobj = Time(lastlimjd, format='jd')
        lastlimfilt = 'ztfr'
        lastlimfiltkey = filtkeys[lastlimfilt]
        lastlimdate = lastlimdateobj.iso.split()[0] + ('%.5f' % (
                lastlimdateobj.mjd - np.floor(lastlimdateobj.mjd)))[1:]
        lastlimmag = 20.5
    else:
        lastlimjd = np.max(photjds[limflags & (photjds < discjd)])
        lastlimdateobj = Time(lastlimjd, format='jd')
        lastlimfilt = photfilters[photjds == lastlimjd][0]
        lastlimfiltkey = filtkeys[lastlimfilt]
        # lastlimdate = lastlimdateobj.iso.split()[0] + ('%.5f' % (
        #         lastlimdateobj.mjd - np.floor(lastlimdateobj.mjd)))[1:]
        lastlimdate = lastlimjd
        lastlimmag = photmaglims[photjds == lastlimjd][0]

    source_props = {}
    '''sourceCoord = SkyCoord(ra = float(source['ra']), dec = float(source['dec']), 
    unit = 'degree', frame = 'icrs') '''
    # source_props['ra'] = {'value': str(ra), 'error': '0.1', 'units': 'arcsec'}
    # source_props['dec'] = {'value': str(dec), 'error': '0.1',
    #                        'units': 'arcsec'}
    source_props['ra'] = {'value': ra}
    source_props['dec'] = {'value': dec}
    # source_props['groupid'] = '48'
    source_props['groupid'] = 48
    source_props['reporter'] = 'V. Karambelkar (Caltech) on behalf of the ZTF ' \
                               'collaboration'
    source_props['discovery_datetime'] = discdate
    source_props['at_type'] = str(int(at_type))
    source_props['host_name'] = ''
    source_props['host_redshift'] = ''
    source_props['transient_redshift'] = ''
    source_props['internal_name'] = internal_name
    source_props['proprietary_period'] = {
        "proprietary_period_value": 0,
        "proprietary_period_units": "years"
    }
    source_props['proprietary_period_groups'] = [48]
    source_props["internal_name_format"] = {
                "prefix": "ZTF",
                "year_format": "YY",
                "postfix": ""
            }

    if remarks is not None:
        source_props['remarks'] = remarks
    # TODO : Get correct exposure times
    source_props['photometry'] = {'photometry_group': {
        '0': {'obsdate': discdate, 'flux': discmag, 'flux_err': discmagerr,
              'limiting_flux': discmaglim, 'flux_units': '1',
              'filter_value': discfiltkey, 'instrument_value': '196', 'exptime': '30',
              'observer': 'None'}}}
    source_props['non_detection'] = {'obsdate': lastlimdate,
                                     'limiting_flux': lastlimmag, 'flux_units': '1',
                                     'filter_value': lastlimfiltkey,
                                     'instrument_value': '196', 'exptime': '30',
                                     'observer': 'None'}
    print(source_props.keys())
    return source_props


def make_json_report(internal_names: list, ras: list, decs: list,
                     at_type: int = 1, remarks=None):
    submit_report = {'at_report': {}}
    for ind in range(len(internal_names)):
        internal_name = internal_names[ind]
        ra = ras[ind]
        dec = decs[ind]
        source_props = get_source_properties_dictionary(internal_name,
                                                        ra,
                                                        dec,
                                                        at_type=at_type,
                                                        remarks=remarks)
        submit_report['at_report'][f"{ind}"] = source_props
    # print(submit_report)
    outjson = 'bulkreport.json'
    with open(outjson, 'w') as j:
        json.dump(submit_report, j)

    with open(outjson, 'r') as f:
        data = f.read()
        submit_report_json = format_to_json(data)
    return submit_report_json


def check_if_we_reported_to_tns(source_name,
                                tns_logfilename='data/tns_reported_log.csv'):
    if not os.path.exists(tns_logfilename):
        return False
    tns_log = pd.read_csv(tns_logfilename)
    return source_name in tns_log['ZTF_names'].values