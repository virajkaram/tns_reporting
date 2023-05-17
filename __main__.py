import argparse
import time

from fritz_utils import query_sources_fritz, api
from datetime import datetime
from astropy.time import Time
import numpy as np
import logging
from tns_utils import check_exists_on_tns, check_if_we_reported_to_tns
import sys
from astropy.table import Table
import os
from tns_utils import make_json_report, send_json_report

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-progIds', default=None,
                        help='Fritz Group IDs, separated by comma')
    parser.add_argument('-query_logfile', default=None,
                        help='Log file with times')
    parser.add_argument('-start_time', default=None)
    parser.add_argument('-end_time', default=Time(datetime.utcnow()).isot)
    parser.add_argument('-timeout_seconds', type=int, default=2)
    parser.add_argument('-test', action='store_true')
    parser.add_argument('-names', type=str, help='Name, if reporting a single source',
                        default=None, nargs='+')
    args = parser.parse_args()

    query_logfile = args.query_logfile
    end_time = args.end_time
    start_time = args.start_time

    reported_logfile = 'reported_sources_tns.txt'
    if args.test:
        reported_logfile = 'reported_sources_sandbox.txt'
    if not os.path.exists(reported_logfile):
        with open(reported_logfile, 'w') as f:
            f.write('ZTF_names\n')

    logger = logging.getLogger()
    handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    if args.progIds is not None:
        if query_logfile is not None:
            if start_time is not None:
                err = f"both time logfile and start time have been specified."
                raise ValueError(err)

            log = Table.read(query_logfile)
            previous_query_times = Time(log['query_end_time'])
            start_time = Time(np.max(previous_query_times.jd), format='jd').isot

        if start_time is None:
            err = f"Please provide a start time, or a logfile."
            raise ValueError(err)

    tot_srcs = 0
    nsrcs_already_on_tns = 0
    nsrcs_reported = 0

    if args.names is not None:
        source_names = np.array(args.names)
        source_ras, source_decs = [], []
        for name in source_names:
            response = api("GET", f"https://fritz.science/api/sources/{name}")
            source = response.json()['data']
            source_ras.append(source['ra'])
            source_decs.append(source['dec'])
        source_decs = np.array(source_decs)
        source_ras = np.array(source_ras)

    if args.progIds is not None:
        prog_ids = args.progIds.split(',')

        # TODO Make this work for multiple prog_ids, currently works only for one
        for prog_id in prog_ids:
            source_list = query_sources_fritz(prog_id,
                                              start_date=start_time,
                                              end_date=end_time,
                                              )
            source_names = np.array([x['id'] for x in source_list])
            source_ras = np.array([x['ra'] for x in source_list])
            source_decs = np.array([x['dec'] for x in source_list])

            logger.info(f"Found {len(source_names)} sources in group {prog_id} "
                        f"saved between {start_time} and {end_time}")

            # tns_source_list = query_sources_fritz(prog_id,
            #                                       start_date=start_time,
            #                                       end_date=end_time,
            #                                       hasTNSname="true")

            # tns_source_names = np.array([x['id'] for x in tns_source_list])
            tns_source_names = []
            logger.info(tns_source_names)
            nontnsmask = [x not in tns_source_names for x in source_names]

            source_names, source_ras, source_decs = source_names[nontnsmask], \
                                                    source_ras[nontnsmask], \
                                                    source_decs[nontnsmask]

            logger.info(f"Found {len(source_names)} sources in group {prog_id} "
                        f"saved between {start_time} and {end_time} without TNS names")

            tot_srcs += len(source_list)

    sources_exist_on_tns = []
    for source_name, source_ra, source_dec in zip(source_names, source_ras,
                                                  source_decs):
        logger.info(f"Querying {source_name}")
        # ztf_source_exists_on_tns, tns_name \
        #     = check_exists_on_tns(source_ra, source_dec, source_name,
        #                           test=args.test)
        ztf_source_exists_on_tns = check_if_we_reported_to_tns(source_name,
                                                               reported_logfile)
        sources_exist_on_tns.append(ztf_source_exists_on_tns)

        if ztf_source_exists_on_tns:
            nsrcs_already_on_tns += 1
            logger.info(f"Source {source_name} exists on TNS.")
        # Timeout to prevent too many requests
        time.sleep(args.timeout_seconds)

    source_names_to_report = source_names[~np.array(sources_exist_on_tns)]
    source_ras_to_report = source_ras[~np.array(sources_exist_on_tns)]
    source_decs_to_report = source_decs[~np.array(sources_exist_on_tns)]
    if len(source_names_to_report) > 0:
        logger.info(f"Reporting sources {source_names_to_report} does "
                    f"not exists on TNS, reporting it.")
        json_report = make_json_report(internal_names=source_names_to_report,
                                       ras=source_ras_to_report,
                                       decs=source_decs_to_report)
        # TODO Remove TEST when sure it works
        if True:
            response = send_json_report(json_report, test=args.test)
            if response.status_code != 200:
                logger.error(f"Error in sending report for {source_name} - "
                             f"{response.status_code}")
            else:
                logger.info(f"Report for {source_name} sent successfully with"
                            f"response {response.text}.")
                nsrcs_reported = len(source_names_to_report)

            with open(f"{reported_logfile}", 'a') as f:
                f.write(f"{source_name}\n")

    if query_logfile:
        new_row = Table()
        programs = "&".join(prog_ids)
        new_row['programs'] = [programs]
        new_row['query_start_time'] = [start_time]
        new_row['query_end_time'] = [end_time]
        new_row['tot_sources'] = [tot_srcs]
        new_row['nsrcs_already_on_TNS'] = [nsrcs_already_on_tns]
        new_row['nsrcs_reported'] = [nsrcs_reported]

        log.add_row([start_time, end_time, programs, tot_srcs, nsrcs_already_on_tns,
                     nsrcs_reported])
        log.write(query_logfile, overwrite=True)
