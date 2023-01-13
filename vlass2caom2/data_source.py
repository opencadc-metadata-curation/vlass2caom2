# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2020.                            (c) 2020.
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

import re
import requests

from bs4 import BeautifulSoup
from collections import defaultdict, deque, OrderedDict
from datetime import datetime
from dateutil import tz
from logging import getLogger
from lxml import etree

from caom2pipe.data_source_composable import DataSource, StateRunnerMeta
from caom2pipe.manage_composable import (
    CadcException,
    get_endpoint_session,
    increment_time,
    query_endpoint_session,
    State,
)
from vlass2caom2 import storage_name


__all__ = ['ContinuumImagingPage', 'NraoPages', 'QuicklookPage', 'WebLogMetadata', 'VLASS_BOOKMARK', 'VLASS_CONTEXT']

VLASS_BOOKMARK = 'vlass_timestamp'
VLASS_CONTEXT = 'vlass_context'


class NraoPages(DataSource):
    """
    Put the NRAO page scraping behind the API for state file-based CAOM record
    creation.
    """

    def __init__(self, config):
        super(NraoPages, self).__init__()
        self._data_sources = []
        self._max_time = datetime.now().astimezone(tz=QuicklookPage.timezone)
        for url in config.data_sources:
            if 'quicklook' in url:
                self._data_sources.append(QuicklookPage(config, url))
            else:
                self._data_sources.append(ContinuumImagingPage(config, url))

    def __str__(self):
        msg = ''
        for data_source in self._data_sources:
            msg = f'{msg}\n{data_source.__class__.__name__}{data_source.__str__()}'
        return msg

    def _capture_todo(self, todo_count):
        self._reporter.capture_todo(todo_count, self._rejected_files, self._skipped_files)
        self._rejected_files = 0
        self._skipped_files = 0

    def _get_all_work(self):
        for data_source in self._data_sources:
            if len(data_source.todo_list) == 0:
                data_source.append_work()

    def get_all_file_urls(self):
        """API for Validation.
        :returns dict with key: fully-qualified url to a file, value: timestamp"""
        self._logger.debug('Begin get_all_file_urls')
        self._get_all_work()
        result = {}
        for data_source in self._data_sources:
            # change a defaultdict(list) with key:timestamp, value list[urls]
            # into a dict with key: url, value: timestamp
            for timestamp, urls in data_source.todo_list.items():
                for url in urls:
                    result[url] = timestamp
            self._logger.error(f'{type(data_source)} {len(data_source.todo_list)} {len(result)}')
        self._logger.debug('End get_all_file_urls')
        self._logger.error(f'len result {len(result)}')
        return result

    def get_time_box_work(self, prev_exec_time, exec_time):
        """
        Time-boxing the file url list returned from the site scrape.

        :param prev_exec_time timestamp start of the timestamp chunk
        :param exec_time timestamp end of the timestamp chunk
        :return: a list of StateRunnerMeta instances, for file names with
            time they were modified
        """
        self._logger.debug('Begin get_time_box_work')
        self._get_all_work()
        self._logger.debug(self)

        temp = deque()
        for data_source in self._data_sources:
            for timestamp in data_source.todo_list:
                if prev_exec_time < timestamp <= exec_time:
                    for entry in data_source.todo_list[timestamp]:
                        temp.append(StateRunnerMeta(entry, timestamp))

            if len(data_source.todo_list) > 0:
                self._max_time = min(self._max_time, data_source.max_time)

        self._capture_todo(len(temp))
        self._logger.debug('End get_time_box_work')
        return temp

    def is_qa_rejected(self, obs_id):
        result = False
        for data_source in self._data_sources:
            for values in data_source.rejected.values():
                for value in values:
                    if obs_id == storage_name.VlassName.get_obs_id_from_file_name(value.split('/')[-2]):
                        result = True
                        break
                if result:
                    break
            if result:
                break
        return result

    def set_start_time(self, start_time):
        """
        Validation requires over-riding the start time from the default value obtained from the bookmark file.
        """
        for data_source in self._data_sources:
            data_source._start_time = start_time

    @property
    def max_time(self):
        return self._max_time


class QuicklookPage(DataSource):

    # timezone for Socorro, NM
    timezone = tz.gettz('US/Mountain')

    def __init__(self, config, base_url):
        super().__init__(None)
        self._data_source_extensions = config.data_source_extensions
        self._epochs = None
        self._session = get_endpoint_session()
        self._state = State(config.state_fqn)
        self._start_time = increment_time(
            self._state.get_bookmark(VLASS_BOOKMARK), 0
        ).astimezone(tz=QuicklookPage.timezone)
        self._todo_list = defaultdict(list)
        self._max_time = None
        self._base_url = base_url
        self._rejected = {}

    def __str__(self):
        return (
            f'\n           start time:: {self._start_time}'
            f'\n             end time:: {self._max_time}'
            f'\nnumber of files found:: {len(self._todo_list)}'
            f'\n      number rejected:: {len(self._rejected)}'
        )

    @property
    def max_time(self):
        return self._max_time

    @property
    def rejected(self):
        return self._rejected

    @property
    def todo_list(self):
        return self._todo_list

    def append_work(self):
        """Predict the URLs for the quicklook images."""
        self._logger.debug('Begin append_work')
        result = defaultdict(list)
        todo_list, max_date = self._build_todo()
        if len(todo_list) > 0:
            for timestamp, urls in todo_list.items():
                for url in urls:
                    # -2 because NRAO URLs always end in /
                    f_prefix = url.split('/')[-2]
                    f1 = f'{url}{f_prefix}.I.iter1.image.pbcor.tt0.rms.subim.fits'
                    f2 = f'{url}{f_prefix}.I.iter1.image.pbcor.tt0.subim.fits'
                    result[timestamp].append(f1)
                    result[timestamp].append(f2)
        self._todo_list = result
        self._max_time = max_date
        self._logger.debug('End append_work')

    def _build_good_todo(self):
        """Create the list of work, based on timestamps from the NRAO
        Quicklook page.

        :return a dict, where keys are timestamps, and values are lists
           of URLs.
        """
        self._logger.debug('Begin _build_good_todo')
        temp = defaultdict(list)
        max_date = self._start_time

        response = None

        try:
            # get the last modified date on the quicklook images listing
            response = query_endpoint_session(self._base_url, self._session)
            if response is None:
                self._logger.warning(f'Could not query {self._base_url}')
            else:
                self._epochs = self._parse_top_page_no_date(response.text)
                self._logger.info(f'Found {len(self._epochs)} epochs on {self._base_url}.')
                response.close()

                for epoch in self._epochs:
                    epoch_url = f'{self._base_url}{epoch}'
                    self._logger.debug(f'Checking epoch {epoch} on date {self._epochs[epoch]}')
                    response = query_endpoint_session(epoch_url, self._session)
                    if response is None:
                        self._logger.warning(f'Could not query epoch {epoch_url}')
                    else:
                        tiles = self._parse_tile_page(response.text)
                        response.close()
                        self._logger.info(f'Found {len(tiles)} tiles on {epoch_url}.')

                        # get the list of tiles
                        for tile in tiles:
                            self._logger.debug(f'Checking tile {tile} with date {tiles[tile]}')
                            tile_url = f'{epoch_url}{tile}'
                            response = query_endpoint_session(tile_url, self._session)
                            if response is None:
                                self._logger.warning(f'Could not query {tile_url}')
                            else:
                                observations = self._parse_id_page(response.text)
                                self._logger.info(f'Found {len(observations)} observations on {tile_url}.')
                                response.close()

                                max_date = self._build_obs_list(temp, observations, max_date, tile_url)
        finally:
            if response is not None:
                response.close()
        self._logger.debug('End _build_good_todo')
        return temp, max_date

    def _build_obs_list(self, temp, observations, max_date, tile_url):
        """
        :param temp: dict with key: timestamp, value: list of fully-qualified URLs
        :param observations: list of partials URLs to look through
        :param max_date: datetime: track the max date that's going to be processed
        :param tile_url: str the initial bit of the URL for looking through
        :return: datetime: max_date found in this list of observations
        """
        # for each tile, get the list of observations
        for observation in observations:
            obs_url = f'{tile_url}{observation}'
            dt_as_s = observations[observation].timestamp()
            temp[dt_as_s].append(obs_url)
        if len(observations.values()) > 0:
            max_date = max(max_date, max(observations.values()))
        return max_date

    def build_qa_rejected_todo(self):
        """
        :return a dict, where keys are timestamps, and values are lists
           of URLs.
        """
        max_date = self._start_time
        response = None
        try:
            for epoch in self._epochs:
                epoch_name = epoch.split('/')[-2]
                epoch_rejected_url = f'{storage_name.QL_URL}{epoch}QA_REJECTED/'
                self._logger.info(f'Checking epoch {epoch_name} on date {self._epochs[epoch]}')
                try:
                    response = query_endpoint_session(epoch_rejected_url, self._session)
                    if response is None:
                        self._logger.warning(f'Could not query epoch {epoch_rejected_url}')
                    else:
                        temp, rejected_max = self._parse_rejected_page(
                            response.text, epoch_name, epoch_rejected_url
                        )
                        max_date = max(max_date, rejected_max)
                        response.close()
                        temp_rejected = self._rejected
                        self._rejected = {**temp, **temp_rejected}
                except CadcException as e:
                    if 'Not Found for url' in str(e):
                        self._logger.info(f'No QA_REJECTED directory for ' f'{epoch_name}. Continuing.')
                    else:
                        raise e
        finally:
            if response is not None:
                response.close()
        return max_date

    def _build_todo(self):
        """Take the list of good files, and the list of rejected files,
        and make them into one todo list.

        :return a dict, where keys are timestamps, and values are lists
           of URLs.
        """
        self._logger.debug(f'Begin build_todo with date {self._start_time}')
        good, good_date = self._build_good_todo()
        self._logger.info(f'{len(good)} good records to process. Check for rejected.')
        rejected_date = self.build_qa_rejected_todo()
        self._logger.info(f'{len(self._rejected)} rejected records to process, date will be {rejected_date}')
        result = OrderedDict()
        for k, v in sorted(sorted(good.items()) + sorted(self._rejected.items())):
            temp = result.setdefault(k, [])
            result[k] = temp + list(set(v))

        if good_date != self._start_time and rejected_date != self._start_time:
            # return the min of the two, because a date from the good list
            # has not necessarily been encountered on the rejected list, and
            # vice-versa
            return_date = min(good_date, rejected_date)
        else:
            return_date = max(good_date, rejected_date)
        num_records = 0
        for key, value in result.items():
            num_records += len(value)
        self._logger.debug(f'End build_todo with {num_records} records total, date {return_date}')
        return result, return_date

    def _parse_id_page(self, html_string):
        """
        :return a dict, where keys are URLs, and values are timestamps
        """
        result = {}
        soup = BeautifulSoup(html_string, features='lxml')
        hrefs = soup.find_all('a', string=re.compile('^VLASS[123]\\.[123]'))
        for ii in hrefs:
            y = ii.get('href')
            z = ii.next_element.next_element.string.replace('-', '').strip()
            dt = QuicklookPage.make_date_time(z)
            if dt >= self._start_time:
                self._logger.debug(f'Adding ID Page: {y}')
                result[y] = dt
        return result

    def _parse_rejected_page(self, html_string, epoch, url):
        """
        :return a dict, where keys are timestamps, and values are lists
           of URLs.
        """
        result = defaultdict(list)
        max_date = self._start_time
        soup = BeautifulSoup(html_string, features='lxml')
        rejected = soup.find_all('a', string=re.compile(epoch.replace('v2', '')))
        for ii in rejected:
            temp = ii.next_element.next_element.string.replace('-', '').strip()
            dt = QuicklookPage.make_date_time(temp)
            if dt >= self._start_time:
                new_url = f'{url}{ii.get_text()}'
                self._logger.debug(f'Adding rejected {new_url}')
                result[dt.timestamp()].append(new_url)
                max_date = max(max_date, dt)
        return result, max_date

    def _parse_specific_rejected_page(self, html_string):
        temp = []
        soup = BeautifulSoup(html_string, features='lxml')
        hrefs = soup.find_all('a', string=re.compile('.fits'))
        for ii in hrefs:
            temp.append(ii.get('href'))
        return temp

    def _parse_tile_page(self, html_string):
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
                dt = QuicklookPage.make_date_time(z)
                if dt >= self._start_time:
                    self._logger.debug(f'Adding Tile Page: {y}')
                    result[y] = dt
        return result

    def _parse_top_page_no_date(self, html_string):
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
                dt = QuicklookPage.make_date_time(z)
                result[y] = dt

        # NRAO introduced a directory named VLASS1.2v2, with this explanation:
        # 1-10-21 - forwarded email from Mark Lacy:
        # The Epoch 1 VLASS quicklook images (VLASS1.1 and VLASS1.2) suffer from
        # a systematic position error that is a function of the zenith distance
        # of the observation, reaching up to 1" in the far south of the survey
        # where zenith distances were largest. These errors are removed in the
        # second epoch quicklook images. We have now applied a correction to
        # the VLASS1.2 images that removes the errors in these too. Corrected
        # images are available in the VLASS1.2v2 directory in
        # archive-new.nrao.edu/vlass/quicklook. The old VLASS1.2 directory will
        # be deprecated. Corrected VLASS1.1 images will be made available later
        # this year.

        # ER 18-10-21
        # My understanding is that the headers were hacked to include updates to
        # astrometry without any other information and the way to determine if
        # v1 or v2 is whether they do-not-have (v1) or have (v2) the extra
        # HISTORY cards.  It’s definitely a decision to not update the DATE card
        # or give any other indication of the v1 or v2 but it is a definitive
        # way to tell them apart.

        # SGo - and because of these two things, ignore top-level directories
        # that have names that start with the same value as other existing
        # directories
        delete_these = []
        for check_this in result:
            for against_this in result:
                if check_this == against_this:
                    continue
                if against_this.startswith(check_this.replace('/', '')):
                    delete_these.append(check_this)

        for entry in delete_these:
            self._logger.warning(f'Ignore content in {entry}')
            del result[entry]

        for entry in result:
            self._logger.info(f'Adding epoch: {entry}')

        return result

    @staticmethod
    def make_date_time(from_str):
        for fmt in [
            '%d%b%Y %H:%M',
            '%Y-%m-%d %H:%M',
            '%Y%m%d %H:%M',
            '%d-%b-%Y %H:%M',
            '%Y_%m_%dT%H_%M_%S.%f',
        ]:
            try:
                dt = datetime.strptime(from_str, fmt)
                break
            except ValueError:
                dt = None
        if dt is None:
            raise CadcException(f'Could not make datetime from {from_str}')
        return dt.astimezone(tz=QuicklookPage.timezone)


class ContinuumImagingPage(QuicklookPage):
    """
    Unlike the quicklook pages, can't reliably predict the Continuum Imaging file names or the URLs for those files. Therefore this class builds
    the list of work to be done by reading the individual
    """

    def __init__(self, config, url):
        super().__init__(config, url)

    def append_work(self):
        """Find the exact URLs for the continuum images."""
        self._logger.debug('Begin append_work')
        self._todo_list, self._max_time = self._build_todo()
        self._logger.info(f'Found {len(self._todo_list)} files, with max_time {self._max_time}')
        self._logger.debug('End append_work')

    def _build_obs_list(self, temp, observations, max_date, tile_url):
        """
        :param temp: dict with key: timestamp, value: list of fully-qualified URLs
        :param observations: list of partials URLs to look through
        :param max_date: datetime: track the max date that's going to be processed
        :param tile_url: str the initial bit of the URL for looking through
        :return: datetime: max_date found in this list of observations
        """
        for observation in observations:
            obs_url = f'{tile_url}{observation}'
            # observations is a dict with key = obs_id, value = datetime
            if observations[observation] >= self._start_time:
                x = self._list_files_on_page(obs_url)
                for key, value in x.items():
                    # switch dict structure from:
                    # key: url, timestamp: value
                    # to
                    # key:timestamp, value: list of urls
                    temp[value.timestamp()].append(f'{obs_url}{key}')
                    max_date = max(max_date, value)
        return max_date

    def _build_todo(self):
        """
        :return a dict, where keys are timestamps, and values are lists
           of URLs.
        """
        self._logger.debug(f'Begin _build_todo with date {self._start_time}')
        good, good_date = self._build_good_todo()
        result = defaultdict(list)
        for timestamp, urls in good.items():
            result[timestamp] += list(set(urls))
        self._logger.debug(f'End _build_todo with {len(result)} records, date {good_date}')
        return result, good_date

    def _list_files_on_page(self, url):
        """:return a dict, where keys are URLS, and values are timestamps, from
        a specific page listing at NRAO."""
        response = None
        try:
            self._logger.debug(f'Querying {url}')
            response = query_endpoint_session(url, self._session)
            if response is None:
                raise CadcException(f'Could not query {url}')
            else:
                result = self._parse_specific_file_list_page(response.text)
                response.close()
                self._logger.info(f'Found {len(result)} files on {url}.')
                return result
        finally:
            if response is not None:
                response.close()

    def _parse_specific_file_list_page(self, html_string):
        """
        :return: a dict, where keys are URLS, and values are timestamps
        """
        result = {}
        soup = BeautifulSoup(html_string, features='lxml')
        for ext in self._data_source_extensions:
            files_list = soup.find_all('a', string=re.compile(f'{ext}'))
            for ii in files_list:
                # looks like 16-Apr-2018 15:43   53M, make it into a datetime
                # for comparison
                temp = ii.next_element.next_element.string.split()
                dt = QuicklookPage.make_date_time(f'{temp[0]} {temp[1]}')
                if dt >= self._start_time:
                    # the hrefs are fully-qualified URLS
                    f_url = ii.get('href')
                    self._logger.debug(f'Adding {f_url} at {dt}')
                    result[f_url] = dt
        return result


class WebLogMetadata:
    def __init__(self, state, session, data_sources):
        self._state = state
        self._session = session
        self._web_log_content = {}
        self._data_sources = data_sources
        self._logger = getLogger(self.__class__.__name__)

    def init_web_log(self):
        """Cache content of https:archive-new.nrao.edu/vlass/weblog, because
        it's large and takes a long time to read. This cached information
        is how time and provenance metadata is found for the individual
        observations.
        """
        epochs = self._state.get_context(VLASS_CONTEXT)
        for key, value in epochs.items():
            epochs[key] = QuicklookPage.make_date_time(value)
            self._logger.info(f'Initialize weblog listing from NRAO for epoch {key} starting at {value}.')
        self.init_web_log_content(epochs)

    def init_web_log_content(self, epochs):
        """
        Cache the listing of weblog processing, because it's really long, and
        takes a long time to read.

        :param epochs: A dict with key == epoch name (e.g. 'VLASS1.1') and
            value = date after which entries are of interest
        """
        if len(self._web_log_content) == 0:
            self._logger.info('Initializing weblog content.')
            # start with no timeout value due to the large number of entries on
            # the page
            for url in self._data_sources:
                # urls look like:
                # https://archive-new.nrao.edu/vlass/weblog/quicklook/
                # https://archive-new.nrao.edu/vlass/weblog/se_continuum_imaging/
                bits = url.split('vlass')
                web_log_url = f'{bits[0]}vlass/weblog{bits[1]}'
                self._logger.debug(f'Querying {web_log_url}')
                with requests.get(web_log_url, stream=True) as r:
                    ctx = etree.iterparse(r.raw, html=True)
                    for event, elem in ctx:
                        if elem.tag == 'a':
                            href = elem.attrib.get('href')
                            for epoch, start_date in epochs.items():
                                # the weblog names don't have the epoch versions in them
                                if href.startswith(epoch.replace('v2', '')):
                                    next_elem = elem.getparent().getnext()
                                    if next_elem is not None:
                                        dt_str = next_elem.text
                                        if dt_str is not None:
                                            dt = QuicklookPage.make_date_time(dt_str.strip())
                                            if dt >= start_date:
                                                fq_url = f'{web_log_url}{href}'
                                                self._web_log_content[fq_url] = dt
                                            break
                        elem.clear()
                    del ctx
        else:
            self._logger.debug('weblog listing already cached.')

    def retrieve_obs_metadata(self, obs_id):
        """Maybe someday this can be done with astroquery, but the VLASS
        metadata isn't in the database that astroquery.Nrao points to, so
        that day is not today."""
        metadata = {}
        mod_obs_id = obs_id.replace('.', '_', 2).replace('_', '.', 1)
        if len(self._web_log_content) == 0:
            self.init_web_log()
        latest_key = None
        max_ts = None
        # there may be multiple processing runs for a single obs id, use the
        # most recent
        # key looks like this:
        # https://archive-new.nrao.edu/vlass/weblog/quicklook/
        # VLASS1.1_T06t31.J203544-183000_P25997v1_2018_03_06T15_51_56.299/
        for key in self._web_log_content:
            # -2 => URLs end with '/'
            temp = key.split('/')[-2]
            if temp.startswith(mod_obs_id):
                dt_bits = '_'.join(ii for ii in temp.replace('/', '').split('_')[3:])
                dt_tz = QuicklookPage.make_date_time(dt_bits).replace(tzinfo=QuicklookPage.timezone)
                if max_ts is None:
                    max_ts = dt_tz
                    latest_key = key
                else:
                    if max_ts < dt_tz:
                        max_ts = dt_tz
                        latest_key = key

        if latest_key is None:
            self._logger.warning(f'Found not observation like {obs_id}.')
        else:
            obs_url = latest_key
            self._logger.debug(f'Querying {obs_url}')
            response = None
            try:
                response = query_endpoint_session(obs_url, self._session)
                if response is None:
                    self._logger.error(f'Could not query {obs_url}')
                else:
                    soup = BeautifulSoup(response.text, features='lxml')
                    response.close()
                    pipeline_bit = soup.find(string=re.compile('pipeline-'))
                    if pipeline_bit is None:
                        self._logger.error(f'Did not find pipeline on {obs_url}')
                    else:
                        pipeline_url = f'{obs_url}{pipeline_bit.strip()}html/index.html'
                        self._logger.debug(f'Querying {pipeline_url}')
                        response = query_endpoint_session(pipeline_url, self._session)
                        if response is None:
                            self._logger.error(f'Could not query {pipeline_url}')
                        else:
                            metadata = self._parse_single_field(response.text)
                            metadata['reference'] = pipeline_url
                            self._logger.debug(f'Setting reference to {pipeline_url}')
                        response.close()
            finally:
                if response is not None:
                    response.close()
        return metadata

    def _parse_single_field(self, html_string):
        result = {}
        soup = BeautifulSoup(html_string, features='lxml')
        for ii in ['Pipeline Version', 'Observation Start', 'Observation End']:
            temp = soup.find(string=re.compile(ii)).next_element.next_element
            result[ii] = temp.get_text().strip()
            self._logger.debug(f'Setting {ii} to {result[ii]}')

        sums = soup.find_all(summary=re.compile('Measurement Set Summaries'))
        if len(sums) == 1:
            tds = sums[0].find_all('td')
            if len(tds) > 7:
                self._logger.debug(f'Setting On Source to {tds[7].string}')
                result['On Source'] = tds[7].string
        # there must be a better way to do this
        result['Observation Start'] = result['Observation Start'].split('\xa0')[0]
        result['Observation End'] = result['Observation End'].split('\xa0')[0]
        return result
