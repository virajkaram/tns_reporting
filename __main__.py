import argparse
import time

from fritz_utils import query_sources_fritz
import pandas as pd
from datetime import datetime
from astropy.time import Time
import numpy as np
import logging
from tns_utils import check_exists_on_tns
import sys
from astropy.table import Table, vstack
from tns_utils import make_json_report, send_json_report

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('progIds', default='43',
                        help='Fritz Group IDs, separated by comma')
    parser.add_argument('-query_logfile', default=None,
                        help='Log file with times')
    parser.add_argument('-start_time', default=None)
    parser.add_argument('-end_time', default=Time(datetime.utcnow()).isot)
    parser.add_argument('-timeout_seconds', default=2)
    parser.add_argument('-test', action='store_true')
    args = parser.parse_args()

    query_logfile = args.query_logfile
    end_time = args.end_time
    start_time = args.start_time

    logger = logging.getLogger()
    handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

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

    prog_ids = args.progIds.split(',')
    tot_srcs = 0
    nsrcs_already_on_tns = 0
    nsrcs_reported = 0

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

        tns_source_list = query_sources_fritz(prog_id,
                                              start_date=start_time,
                                              end_date=end_time,
                                              hasTNSname="true")

        tns_source_names = np.array([x['id'] for x in tns_source_list])
        logger.info(tns_source_names)
        nontnsmask = [x not in tns_source_names for x in source_names]

        source_names, source_ras, source_decs = source_names[nontnsmask], \
                                                source_ras[nontnsmask], \
                                                source_decs[nontnsmask]

        logger.info(f"Found {len(source_names)} sources in group {prog_id} "
                    f"saved between {start_time} and {end_time} without TNS names")

        tot_srcs += len(source_list)
        for source_name, source_ra, source_dec in zip(source_names, source_ras,
                                                      source_decs):
            logger.info(f"Querying {source_name}")
            ztf_source_exists_on_tns, tns_name \
                = check_exists_on_tns(source_ra, source_dec, source_name,
                                      test=args.test)

            if not ztf_source_exists_on_tns:
                logger.info(f"Source {source_name} does not exists on TNS, "
                            f"reporting it.")
                json_report = make_json_report(source_name, ra=source_ra,
                                               dec=source_dec)
                # TODO Remove TEST when sure it works
                if args.test:
                    response = send_json_report(json_report, test=args.test)
                    if response.status_code != 200:
                        logger.error(f"Error in sending report for {source_name} - "
                                     f"{response.status_code}")
                    else:
                        logger.info(f"Report for {source_name} sent successfully with"
                                    f"response {response.text}.")
                        nsrcs_reported += 1
            else:
                nsrcs_already_on_tns += 1
                logger.info(f"Source {source_name} exists on TNS, "
                            f"as {tns_name}.")
            # Timeout to prevent too many requests
            time.sleep(args.timeout_seconds)

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
