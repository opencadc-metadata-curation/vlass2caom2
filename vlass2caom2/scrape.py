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

from datetime import datetime

from bs4 import BeautifulSoup

from caom2pipe import manage_composable as mc

__all__ = ['build_good_todo', 'retrieve_obs_metadata',
           'build_qa_rejected_todo', 'PAGE_TIME_FORMAT', 'query_top_page',
           'list_files_on_page']


PAGE_TIME_FORMAT = '%d%b%Y %H:%M'
QL_URL = 'https://archive-new.nrao.edu/vlass/quicklook/'
QL_WEB_LOG_URL = 'https://archive-new.nrao.edu/vlass/weblog/quicklook/'

web_log_content = {}


def _parse_id_page(html_string, epoch, start_date):
    """
    :return a dict, where keys are URLs, and values are timestamps
    """
    result = {}
    soup = BeautifulSoup(html_string, features='lxml')
    hrefs = soup.find_all('a')
    for ii in hrefs:
        y = ii.get('href')
        if y.startswith(epoch):
            z = ii.next_element.next_element.string.replace('-', '').strip()
            dt = datetime.strptime(z, PAGE_TIME_FORMAT)
            if dt >= start_date:
                logging.info('Adding ID Page: {}'.format(y))
                result[y] = dt
    return result


def _parse_field_page(html_string, start_date):
    """
    :return a dict, where keys are URLs, and values are timestamps
    """
    result = {}
    soup = BeautifulSoup(html_string, features='lxml')
    hrefs = soup.find_all('a')
    for ii in hrefs:
        y = ii.get('href')
        if y.startswith('T'):
            z = ii.next_element.next_element.string.replace('-', '').strip()
            dt = datetime.strptime(z, PAGE_TIME_FORMAT)
            if dt >= start_date:
                logging.info('Adding Field Page: {}'.format(y))
                result[y] = dt
    return result


def _parse_top_page(html_string, start_date):
    """
    :return a dict, where keys are URLs, and values are timestamps
    """
    result = {}
    soup = BeautifulSoup(html_string, features='lxml')
    hrefs = soup.find_all('a')
    for ii in hrefs:
        y = ii.get('href')
        if y.startswith('VLASS') and y.endswith('/'):
            z = ii.next_element.next_element.string.replace('-', '').strip()
            dt = datetime.strptime(z, PAGE_TIME_FORMAT)
            if dt >= start_date:
                logging.info('Adding epoch: {}'.format(y))
                result[y] = dt
    return result


def build_good_todo(start_date):
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
        response = mc.query_endpoint(QL_URL)
        if response is None:
            logging.warning('Could not query {}'.format(QL_URL))
        else:
            epochs = _parse_top_page(response.text, start_date)
            response.close()

            for epoch in epochs:
                epoch_url = '{}{}'.format(QL_URL, epoch)
                logging.info(
                    'Checking epoch {} on date {}'.format(
                        epoch, epochs[epoch]))
                response = mc.query_endpoint(epoch_url)
                if response is None:
                    logging.warning(
                        'Could not query epoch {}'.format(epoch_url))
                else:
                    fields = _parse_field_page(response.text, start_date)
                    response.close()

                    # get the list of fields
                    for field in fields:
                        logging.info('Checking field {} with date {}'.format(
                            field, fields[field]))
                        field_url = '{}{}'.format(epoch_url, field)
                        response = mc.query_endpoint(field_url)
                        if response is None:
                            logging.warning(
                                'Could not query {}'.format(field_url))
                        else:
                            observations = _parse_id_page(
                                response.text, epoch.strip('/'), start_date)
                            response.close()

                            # for each field, get the list of observations
                            for observation in observations:
                                obs_url = '{}{}'.format(field_url, observation)
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


def _parse_single_field(html_string):
    result = {}
    soup = BeautifulSoup(html_string, features='lxml')
    for ii in ['Pipeline Version', 'Observation Start', 'Observation End']:
        temp = soup.find(string=re.compile(ii)).next_element.next_element
        result[ii] = temp.get_text().strip()
        logging.debug('Setting {} to {}'.format(ii, result[ii]))

    sums = soup.find_all(summary=re.compile('Measurement Set Summaries'))
    if len(sums) == 1:
        tds = sums[0].find_all('td')
        if len(tds) > 7:
            logging.debug('Setting On Source to {}'.format(tds[7].string))
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
        dt = datetime.strptime(temp, PAGE_TIME_FORMAT)
        if dt >= start_date:
            new_url = '{}{}'.format(url, ii.get_text())
            logging.debug('Adding rejected {}'.format(new_url))
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


def build_qa_rejected_todo(start_date):
    """
    :return a dict, where keys are timestamps, and values are lists
       of URLs.
    """
    rejected = {}
    max_date = start_date

    response = None
    try:
        # get the last modified date on the quicklook images listing
        response = mc.query_endpoint(QL_URL)
        if response is None:
            logging.warning('Could not query {}'.format(QL_URL))
        else:
            epochs = _parse_top_page(response.text, start_date)
            response.close()

            for epoch in epochs:
                epoch_name = epoch.split('/')[-2]
                epoch_rejected_url = '{}{}QA_REJECTED/'.format(QL_URL, epoch)
                logging.info(
                    'Checking epoch {} on date {}'.format(
                        epoch_name, epochs[epoch]))
                response = mc.query_endpoint(epoch_rejected_url)
                if response is None:
                    logging.warning(
                        'Could not query epoch {}'.format(epoch_rejected_url))
                else:
                    temp, rejected_max = _parse_rejected_page(
                        response.text, epoch_name, start_date,
                        epoch_rejected_url)
                    max_date = max(start_date, rejected_max)
                    response.close()
                    temp_rejected = rejected
                    rejected = {**temp, **temp_rejected}
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
    logging.debug('Being build_todo with date {}'.format(start_date))
    good, good_date = build_good_todo(start_date)
    logging.info('{} good records to process.'.format(len(good)))
    rejected, rejected_date = build_qa_rejected_todo(start_date)
    logging.info(
        '{} rejected records to process, date will be {}'.format(
            len(rejected), rejected_date))
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
    logging.debug('End build_todo with {} records, date {}'.format(
        len(result), return_date))
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
        dt = datetime.strptime(z, PAGE_TIME_FORMAT)
        if dt >= start_date:
            logging.info('Adding {}'.format(y))
            result[y] = dt
    return result


def _parse_specific_file_list_page(html_string):
    """
    :return: list, where entries are file names found on the page
        that lists them for a specific field
    """
    result = []
    soup = BeautifulSoup(html_string, features='lxml')
    fits_files = soup.find_all('a', string=re.compile('\\.fits'))
    for ii in fits_files:
        logging.debug('fits file is {}'.format(ii.get('href')))
        result.append(ii.get('href'))
    return result


def list_files_on_page(url):
    """:return a list of URLS for .fits files on a page, from
    a specific page listing at NRAO."""
    response = None
    try:
        logging.debug('Querying {}'.format(url))
        response = mc.query_endpoint(url)
        if response is None:
            raise mc.CadcException('Could not query {}'.format(url))
        else:
            result = _parse_specific_file_list_page(response.text)
            response.close()
            return result
    finally:
        if response is not None:
            response.close()


def init_web_log_content(epochs):
    """
    :param epochs: A dict with key == epoch name (e.g. 'VLASS1.1') and
        value = date after which entries are of interest
    """
    logging.info('Initializing weblog content.')
    response = None
    try:
        response = mc.query_endpoint(QL_WEB_LOG_URL, timeout=360)
        if response is None:
            raise mc.CadcException(
                'Need access to {}'.format(QL_WEB_LOG_URL))
        global web_log_content
        for ii in epochs:
            temp_orig = web_log_content
            temp = _parse_page_for_hrefs(response.text, ii, epochs[ii])
            web_log_content = {**temp_orig, **temp}
        response.close()
    finally:
        if response is not None:
            response.close()


def retrieve_obs_metadata(obs_id):
    """Maybe someday this can be done with astroquery, but the VLASS
    metadata isn't in the database that astroquery.Nrao points to, so
    that day is not today."""
    metadata = {}
    mod_obs_id = obs_id.replace('.', '_', 2).replace('_', '.', 1)
    global web_log_content
    if len(web_log_content) == 0:
        raise mc.CadcException('Must initialize weblog content.')
    for key in web_log_content.keys():
        if key.startswith(mod_obs_id):
            obs_url = '{}{}'.format(QL_WEB_LOG_URL, key)
            logging.debug('Querying {}'.format(obs_url))
            response = None
            try:
                response = mc.query_endpoint(obs_url)
                if response is None:
                    logging.error('Could not query {}'.format(obs_url))
                else:
                    pipeline_bit = _parse_for_reference(response.text,
                                                        'pipeline-')
                    response.close()
                    if pipeline_bit is None:
                        logging.error(
                            'Did not find pipeline on {}'.format(obs_url))
                    else:
                        pipeline_url = '{}{}html/index.html'.format(
                            obs_url, pipeline_bit.strip())
                        logging.debug('Querying {}'.format(pipeline_url))
                        response = mc.query_endpoint(pipeline_url)
                        if response is None:
                            logging.error(
                                'Could not query {}'.format(pipeline_url))
                        else:
                            metadata = _parse_single_field(response.text)
                            metadata['reference'] = pipeline_url
                            logging.debug(
                                'Setting reference to {}'.format(pipeline_url))
                        response.close()
            finally:
                if response is not None:
                    response.close()
    return metadata


def build_file_url_list(start_time):
    """
    :return a dict, where keys are URLs, and values are timestamps
    """
    result = {}
    todo_list, max_date = build_todo(start_time)
    if len(todo_list) > 0:
        for timestamp, urls in todo_list.items():
            result[timestamp] = []
            for url in urls:
                # -2 because NRAO URLs always end in /
                f_prefix = url.split('/')[-2]
                f1 = '{}{}.I.iter1.image.pbcor.tt0.rms.subim.fits'.format(
                        url, f_prefix)
                f2 = '{}{}.I.iter1.image.pbcor.tt0.subim.fits'.format(
                        url, f_prefix)
                result[timestamp].append(f1)
                result[timestamp].append(f2)
    return result, max_date


def query_top_page():
    """Query the timestamp from the top page, for reporting.
    """
    start_date = datetime.strptime('01Jan2017 12:00', PAGE_TIME_FORMAT)
    response = None

    try:
        # get the last modified date on the quicklook images listing
        response = mc.query_endpoint(QL_URL)
        if response is None:
            logging.warning('Could not query {}'.format(QL_URL))
        else:
            epochs = _parse_top_page(response.text, start_date)
            for key, value in epochs.items():
                logging.error('{} {}'.format(
                    key, datetime.strftime(value, PAGE_TIME_FORMAT)))
    finally:
        if response is not None:
            response.close()
