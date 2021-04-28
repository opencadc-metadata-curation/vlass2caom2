# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2019.                            (c) 2019.
#  Government of Canada                 Gouvernement du Canada
#  National Research Council            Conseil national de recherches
#  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
#  All rights reserved                  Tous droits réservés
#
#  NRC disclaims any warranties,        Le CNRC dénie toute garantie
#  expressed, implied, or               énoncée, implicite ou légale,
#  statutory, of any kind with          de quelque nature que ce
#  respect to the software,             soit, concernant le logiciel,
#  including without limitation         y compris sans restriction
#  any warranty of merchantability      toute garantie de valeur
#  or fitness for a particular          marchande ou de pertinence
#  purpose. NRC shall not be            pour un usage particulier.
#  liable in any event for any          Le CNRC ne pourra en aucun cas
#  damages, whether direct or           être tenu responsable de tout
#  indirect, special or general,        dommage, direct ou indirect,
#  consequential or incidental,         particulier ou général,
#  arising from the use of the          accessoire ou fortuit, résultant
#  software.  Neither the name          de l'utilisation du logiciel. Ni
#  of the National Research             le nom du Conseil National de
#  Council of Canada nor the            Recherches du Canada ni les noms
#  names of its contributors may        de ses  participants ne peuvent
#  be used to endorse or promote        être utilisés pour approuver ou
#  products derived from this           promouvoir les produits dérivés
#  software without specific prior      de ce logiciel sans autorisation
#  written permission.                  préalable et particulière
#                                       par écrit.
#
#  This file is part of the             Ce fichier fait partie du projet
#  OpenCADC project.                    OpenCADC.
#
#  OpenCADC is free software:           OpenCADC est un logiciel libre ;
#  you can redistribute it and/or       vous pouvez le redistribuer ou le
#  modify it under the terms of         modifier suivant les termes de
#  the GNU Affero General Public        la “GNU Affero General Public
#  License as published by the          License” telle que publiée
#  Free Software Foundation,            par la Free Software Foundation
#  either version 3 of the              : soit la version 3 de cette
#  License, or (at your option)         licence, soit (à votre gré)
#  any later version.                   toute version ultérieure.
#
#  OpenCADC is distributed in the       OpenCADC est distribué
#  hope that it will be useful,         dans l’espoir qu’il vous
#  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
#  without even the implied             GARANTIE : sans même la garantie
#  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
#  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
#  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
#  General Public License for           Générale Publique GNU Affero
#  more details.                        pour plus de détails.
#
#  You should have received             Vous devriez avoir reçu une
#  a copy of the GNU Affero             copie de la Licence Générale
#  General Public License along         Publique GNU Affero avec
#  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
#  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
#                                       <http://www.gnu.org/licenses/>.
#
#  $Revision: 4 $
#
# ***********************************************************************
#

import collections
import logging
import re
import requests

from datetime import datetime
from dateutil import tz
from lxml import etree

from bs4 import BeautifulSoup

from caom2pipe import manage_composable as mc

__all__ = ['build_good_todo', 'make_date_time', 'retrieve_obs_metadata',
           'build_qa_rejected_todo', 'query_top_page',
           'list_files_on_page', 'build_file_url_list', 'build_url_list']


QL_URL = 'https://archive-new.nrao.edu/vlass/quicklook/'
QL_WEB_LOG_URL = 'https://archive-new.nrao.edu/vlass/weblog/quicklook/'

web_log_content = {}


def make_date_time(from_str):
    for fmt in ['%d%b%Y %H:%M', '%Y-%m-%d %H:%M', '%Y%m%d %H:%M',
                '%d-%b-%Y %H:%M', '%Y_%m_%dT%H_%M_%S.%f']:
        try:
            dt = datetime.strptime(from_str, fmt)
            break
        except ValueError:
            dt = None
    if dt is None:
        raise mc.CadcException(f'Could not make datetime from {from_str}')
    return dt


def _parse_id_page(html_string, start_date):
    """
    :return a dict, where keys are URLs, and values are timestamps
    """
    result = {}
    soup = BeautifulSoup(html_string, features='lxml')
    hrefs = soup.find_all('a', string=re.compile('^VLASS[123]\\.[123]'))
    for ii in hrefs:
        y = ii.get('href')
        z = ii.next_element.next_element.string.replace('-', '').strip()
        dt = make_date_time(z)
        if dt >= start_date:
            logging.info(f'Adding ID Page: {y}')
            result[y] = dt
    return result


def _parse_tile_page(html_string, start_date):
    """
    Parse the page which lists the tiles viewed during an epoch.

    :return a dict, where keys are URLs, and values are timestamps
    """
    result = {}
    soup = BeautifulSoup(html_string, features='lxml')
    hrefs = soup.find_all('a')
    for ii in hrefs:
        y = ii.get('href')
        if y.startswith('T'):
            z = ii.next_element.next_element.string.replace('-', '').strip()
            dt = make_date_time(z)
            if dt >= start_date:
                logging.info(f'Adding Tile Page: {y}')
                result[y] = dt
    return result


def _parse_top_page(html_string, start_date):
    """
    Parse the page which lists the epochs.

    :return a dict, where keys are URLs, and values are timestamps
    """
    result = {}
    soup = BeautifulSoup(html_string, features='lxml')
    hrefs = soup.find_all('a')
    for ii in hrefs:
        y = ii.get('href')
        if y.startswith('VLASS') and y.endswith('/'):
            z = ii.next_element.next_element.string.replace('-', '').strip()
            dt = make_date_time(z)
            if dt >= start_date:
                logging.info(f'Adding epoch: {y}')
                result[y] = dt
    return result


def _parse_top_page_no_date(html_string):
    """
    Parse the page which lists the epochs.

    :return a dict, where keys are URLs, and values are timestamps
    """
    result = {}
    soup = BeautifulSoup(html_string, features='lxml')
    hrefs = soup.find_all('a')
    for ii in hrefs:
        y = ii.get('href')
        if y.startswith('VLASS') and y.endswith('/'):
            z = ii.next_element.next_element.string.replace('-', '').strip()
            dt = make_date_time(z)
            logging.info(f'Adding epoch: {y}')
            result[y] = dt
    return result


def build_good_todo(start_date, session):
    """Create the list of work, based on timestamps from the NRAO
    Quicklook page.

    :return a dict, where keys are timestamps, and values are lists
       of URLs.
    """
    temp = {}
    max_date = start_date

    response = None

    try:
        # get the last modified date on the quicklook images listing
        response = mc.query_endpoint_session(QL_URL, session)
        if response is None:
            logging.warning(f'Could not query {QL_URL}')
        else:
            epochs = _parse_top_page_no_date(response.text)
            response.close()

            for epoch in epochs:
                epoch_url = f'{QL_URL}{epoch}'
                logging.info(f'Checking epoch {epoch} on date {epochs[epoch]}')
                response = mc.query_endpoint_session(epoch_url, session)
                if response is None:
                    logging.warning(f'Could not query epoch {epoch_url}')
                else:
                    tiles = _parse_tile_page(response.text, start_date)
                    response.close()

                    # get the list of tiles
                    for tile in tiles:
                        logging.info(
                            f'Checking tile {tile} with date {tiles[tile]}'
                        )
                        tile_url = f'{epoch_url}{tile}'
                        response = mc.query_endpoint_session(tile_url, session)
                        if response is None:
                            logging.warning(f'Could not query {tile_url}')
                        else:
                            observations = _parse_id_page(
                                response.text, start_date)
                            response.close()

                            # for each tile, get the list of observations
                            for observation in observations:
                                obs_url = f'{tile_url}{observation}'
                                dt_as_s = observations[observation].timestamp()
                                max_date = max(
                                    max_date, observations[observation])
                                if dt_as_s in temp:
                                    temp[dt_as_s].append(obs_url)
                                else:
                                    temp[dt_as_s] = [obs_url]
    finally:
        if response is not None:
            response.close()
    return temp, max_date


def _parse_for_reference(html_string, reference):
    soup = BeautifulSoup(html_string, features='lxml')
    return soup.find(string=re.compile(reference))


def _parse_image_phase_centre_list_page(html_string):
    """
    :param html_string:
    :return: a list of all hrefs on the page
    """
    result = []
    soup = BeautifulSoup(html_string, features='lxml')
    hrefs = soup.find_all('a', string=re.compile('^VLASS[123]\\.[123]'))
    for href in hrefs:
        result.append(href.get('href'))
    return result


def _parse_single_field(html_string):
    result = {}
    soup = BeautifulSoup(html_string, features='lxml')
    for ii in ['Pipeline Version', 'Observation Start', 'Observation End']:
        temp = soup.find(string=re.compile(ii)).next_element.next_element
        result[ii] = temp.get_text().strip()
        logging.debug(f'Setting {ii} to {result[ii]}')

    sums = soup.find_all(summary=re.compile('Measurement Set Summaries'))
    if len(sums) == 1:
        tds = sums[0].find_all('td')
        if len(tds) > 7:
            logging.debug(f'Setting On Source to {tds[7].string}')
            result['On Source'] = tds[7].string
    # there must be a better way to do this
    result['Observation Start'] = result['Observation Start'].split('\xa0')[0]
    result['Observation End'] = result['Observation End'].split('\xa0')[0]
    return result


def _parse_rejected_page(html_string, epoch, start_date, url):
    """
    :return a dict, where keys are timestamps, and values are lists
       of URLs.
    """
    result = {}
    max_date = start_date
    soup = BeautifulSoup(html_string, features='lxml')
    rejected = soup.find_all('a', string=re.compile(epoch))
    for ii in rejected:
        temp = ii.next_element.next_element.string.replace('-', '').strip()
        dt = make_date_time(temp)
        if dt >= start_date:
            new_url = f'{url}{ii.get_text()}'
            logging.debug(f'Adding rejected {new_url}')
            if dt.timestamp() in result:
                result[dt.timestamp()].append(new_url)
            else:
                result[dt.timestamp()] = [new_url]
            max_date = max(max_date, dt)
    return result, max_date


def _parse_specific_rejected_page(html_string):
    temp = []
    soup = BeautifulSoup(html_string, features='lxml')
    hrefs = soup.find_all('a', string=re.compile('.fits'))
    for ii in hrefs:
        temp.append(ii.get('href'))
    return temp


def build_qa_rejected_todo(start_date, session):
    """
    :return a dict, where keys are timestamps, and values are lists
       of URLs.
    """
    rejected = {}
    max_date = start_date

    response = None
    try:
        # get the last modified date on the quicklook images listing
        response = mc.query_endpoint_session(QL_URL, session)
        if response is None:
            logging.warning(f'Could not query {QL_URL}')
        else:
            epochs = _parse_top_page_no_date(response.text)
            response.close()

            for epoch in epochs:
                epoch_name = epoch.split('/')[-2]
                epoch_rejected_url = f'{QL_URL}{epoch}QA_REJECTED/'
                logging.info(
                    f'Checking epoch {epoch_name} on date {epochs[epoch]}'
                )
                try:
                    response = mc.query_endpoint_session(
                        epoch_rejected_url, session
                    )
                    if response is None:
                        logging.warning(
                            f'Could not query epoch {epoch_rejected_url}'
                        )
                    else:
                        temp, rejected_max = _parse_rejected_page(
                            response.text, epoch_name, start_date,
                            epoch_rejected_url)
                        max_date = max(start_date, rejected_max)
                        response.close()
                        temp_rejected = rejected
                        rejected = {**temp, **temp_rejected}
                except mc.CadcException as e:
                    if 'Not Found for url' in str(e):
                        logging.info(f'No QA_REJECTED directory for '
                                     f'{epoch_name}. Continuing.')
                    else:
                        raise e
    finally:
        if response is not None:
            response.close()
    return rejected, max_date


def build_todo(start_date):
    """Take the list of good files, and the list of rejected files,
    and make them into one todo list.

    :return a dict, where keys are timestamps, and values are lists
       of URLs.
    """
    logging.debug(f'Begin build_todo with date {start_date}')
    session = mc.get_endpoint_session()
    good, good_date = build_good_todo(start_date, session)
    logging.info(f'{len(good)} good records to process. Check for rejected.')
    rejected, rejected_date = build_qa_rejected_todo(start_date, session)
    logging.info(
        f'{len(rejected)} rejected records to process, date will be '
        f'{rejected_date}'
    )
    result = collections.OrderedDict()
    for k, v in sorted(sorted(good.items()) + sorted(rejected.items())):
        temp = result.setdefault(k, [])
        result[k] = temp + list(set(v))

    if good_date != start_date and rejected_date != start_date:
        # return the min of the two, because a date from the good list
        # has not necessarily been encountered on the rejected list, and
        # vice-versa
        return_date = min(good_date, rejected_date)
    else:
        return_date = max(good_date, rejected_date)
    logging.debug(
        f'End build_todo with {len(result)} records, date {return_date}'
    )
    return result, return_date


def _parse_page_for_hrefs(html_string, reference, start_date):
    """
    :return a dict, where keys are URLs, and values are timestamps
    """
    result = {}
    soup = BeautifulSoup(html_string, features='lxml')
    hrefs = soup.find_all('a', string=re.compile(reference))
    for ii in hrefs:
        y = ii.get('href')
        z = ii.next_element.next_element.string.replace('-', '').strip()
        dt = make_date_time(z)
        if dt >= start_date:
            logging.info(f'Adding {y}')
            result[y] = dt
    return result


def _parse_specific_file_list_page(html_string, start_time):
    """
    :return: a dict, where keys are URLS, and values are timestamps
    """
    result = {}
    soup = BeautifulSoup(html_string, features='lxml')
    fits_files = soup.find_all('a', string=re.compile('\\.fits'))
    for ii in fits_files:
        # looks like 16-Apr-2018 15:43   53M, make it into a datetime
        # for comparison
        temp = ii.next_element.next_element.string.split()
        dt = make_date_time(f'{temp[0]} {temp[1]}')
        if dt >= start_time:
            # the hrefs are fully-qualified URLS
            f_url = ii.get('href')
            logging.info(f'Adding {f_url}')
            result[f_url] = dt
    return result


def list_files_on_page(url, start_time, session):
    """:return a dict, where keys are URLS, and values are timestamps, from
    a specific page listing at NRAO."""
    response = None
    try:
        logging.debug(f'Querying {url}')
        response = mc.query_endpoint_session(url, session)
        if response is None:
            raise mc.CadcException(f'Could not query {url}')
        else:
            result = _parse_specific_file_list_page(response.text,
                                                    start_time)
            response.close()
            return result
    finally:
        if response is not None:
            response.close()


def init_web_log_content(epochs):
    """
    Cache the listing of weblog processing, because it's really long, and
    takes a long time to read.

    :param epochs: A dict with key == epoch name (e.g. 'VLASS1.1') and
        value = date after which entries are of interest
    """
    global web_log_content
    if len(web_log_content) == 0:
        logging.info('Initializing weblog content.')
        # start with no timeout value due to the large number of entries on
        # the page
        with requests.get(QL_WEB_LOG_URL, stream=True) as r:
            ctx = etree.iterparse(r.raw, html=True, tag='a')
            for event, elem in ctx:
                href = elem.attrib.get('href')
                for epoch, start_date in epochs.items():
                    if href.startswith(epoch):
                        if elem.tail is not None:
                            dt = make_date_time(
                                elem.tail.replace('-', '').strip()
                            )
                            if dt >= start_date:
                                web_log_content[href] = dt
                            break
                elem.clear()
    else:
        logging.debug('weblog listing already cached.')


def retrieve_obs_metadata(obs_id):
    """Maybe someday this can be done with astroquery, but the VLASS
    metadata isn't in the database that astroquery.Nrao points to, so
    that day is not today."""
    metadata = {}
    mod_obs_id = obs_id.replace('.', '_', 2).replace('_', '.', 1)
    global web_log_content
    if len(web_log_content) == 0:
        raise mc.CadcException('Must initialize weblog content.')
    latest_key = None
    max_ts = None
    tz_info = tz.gettz('US/Socorro')
    # there may be multiple processing runs for a single obs id, use the
    # most recent
    for key in web_log_content.keys():
        if key.startswith(mod_obs_id):
            dt_bits = '_'.join(ii for ii in
                               key.replace('/', '').split('_')[3:])
            dt_tz = make_date_time(dt_bits).replace(tzinfo=tz_info)
            if max_ts is None:
                max_ts = dt_tz
                latest_key = key
            else:
                if max_ts < dt_tz:
                    max_ts = dt_tz
                    latest_key = key

    session = mc.get_endpoint_session()
    if latest_key is not None:
        obs_url = f'{QL_WEB_LOG_URL}{latest_key}'
        logging.debug(f'Querying {obs_url}')
        response = None
        try:
            response = mc.query_endpoint_session(obs_url, session)
            if response is None:
                logging.error(f'Could not query {obs_url}')
            else:
                pipeline_bit = _parse_for_reference(response.text,
                                                    'pipeline-')
                response.close()
                if pipeline_bit is None:
                    logging.error(f'Did not find pipeline on {obs_url}')
                else:
                    pipeline_url = \
                        f'{obs_url}{pipeline_bit.strip()}html/index.html'
                    logging.debug(f'Querying {pipeline_url}')
                    response = mc.query_endpoint_session(pipeline_url, session)
                    if response is None:
                        logging.error(f'Could not query {pipeline_url}')
                    else:
                        metadata = _parse_single_field(response.text)
                        metadata['reference'] = pipeline_url
                        logging.debug(f'Setting reference to {pipeline_url}')
                    response.close()
        finally:
            if response is not None:
                response.close()
    return metadata


def build_file_url_list(start_time):
    """
    :return a dict, where values are URLs, and keys are timestamps
    """
    result = {}
    todo_list, max_date = build_todo(start_time)
    if len(todo_list) > 0:
        for timestamp, urls in todo_list.items():
            result[timestamp] = []
            for url in urls:
                # -2 because NRAO URLs always end in /
                f_prefix = url.split('/')[-2]
                f1 = f'{url}{f_prefix}.I.iter1.image.pbcor.tt0.rms.subim.fits'
                f2 = f'{url}{f_prefix}.I.iter1.image.pbcor.tt0.subim.fits'
                result[timestamp].append(f1)
                result[timestamp].append(f2)
    return result, max_date


def build_url_list(start_date):
    """
    Differs from build_file_url_list in that it loads the page and checks the
    timestamps for the file individually, as opposed to trusting that the
    source location directory timestamps are representative.

    :return a dict, where keys are URLs, and values are timestamps
    """
    result = {}
    session = mc.get_endpoint_session()
    todo_list, ignore_date = build_good_todo(start_date, session)
    rejected, ignore_date = build_qa_rejected_todo(start_date, session)
    for coll in [todo_list, rejected]:
        if len(coll) > 0:
            for timestamp, urls in coll.items():
                for url in urls:
                    temp = list_files_on_page(url, start_date, session)
                    for key, value in temp.items():
                        # key f_name
                        # value timestamp
                        temp_url = f'{url}{key}'
                        result[temp_url] = value.timestamp()
    return result


def query_top_page():
    """Query the timestamp from the top page, for reporting.
    """
    start_date = make_date_time('01Jan2017 12:00')
    response = None
    max_date = None

    try:
        # get the last modified date on the quicklook images listing
        session = mc.get_endpoint_session()
        response = mc.query_endpoint_session(QL_URL, session)
        if response is None:
            logging.warning(f'Could not query {QL_URL}')
        else:
            epochs = _parse_top_page(response.text, start_date)
            for key, value in epochs.items():
                logging.info(f'{key} {make_date_time(value)}')
                if max_date is None:
                    max_date = value
                else:
                    max_date = max(max_date, value)
    finally:
        if response is not None:
            response.close()

    return max_date
