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

"""
NRAO introduced a directory named VLASS1.2v2, with this explanation:

1-10-21 - forwarded email from Mark Lacy:
The Epoch 1 VLASS quicklook images (VLASS1.1 and VLASS1.2) suffer from a systematic position error that is a function
of the zenith distance of the observation, reaching up to 1" in the far south of the survey where zenith distances
were largest. These errors are removed in the second epoch quicklook images. We have now applied a correction to
the VLASS1.2 images that removes the errors in these too. Corrected images are available in the VLASS1.2v2 directory in
archive-new.nrao.edu/vlass/quicklook. The old VLASS1.2 directory will be deprecated. Corrected VLASS1.1 images will be
made available later this year.

ER 18-10-21
My understanding is that the headers were hacked to include updates to astrometry without any other information and
the way to determine if v1 or v2 is whether they do-not-have (v1) or have (v2) the extra HISTORY cards.  It’s
definitely a decision to not update the DATE card or give any other indication of the v1 or v2 but it is a definitive
way to tell them apart.

SGo - and because of these two things, ignore top-level directories that have names that start with the same value
as other existing directories

"""

import re
import requests

from bs4 import BeautifulSoup
from collections import deque
from datetime import datetime
from logging import getLogger
from lxml import etree

from caom2pipe.data_source_composable import DataSource, HttpDataSource, StateRunnerMeta
from caom2pipe.manage_composable import (
    CadcException,
    get_endpoint_session,
    make_datetime,
    query_endpoint_session,
    State,
)
from vlass2caom2 import storage_name


__all__ = [
    'filter_by_epoch',
    'filter_by_epoch_name',
    'filter_by_tile',
    'NraoPages',
    'VlassImagePage',
    'WebLogMetadata',
    'VLASS_CONTEXT',
]

VLASS_CONTEXT = 'vlass_context'


def filter_by_epoch(href):
    return re.search('VLASS[123]\\.[123]', href)


def filter_by_epoch_name(href):
    return href.startswith('VLASS') and href.endswith('/')


def filter_by_tile(href):
    return href.startswith('T') or href == 'QA_REJECTED'


class NraoPages(DataSource):
    """
    Put the NRAO page scraping behind the API for state file-based CAOM record
    creation.
    """

    def __init__(self, config, start_dt=datetime(year=2018, month=1, day=1)):
        #  01-Jan-2018 00:00 is the start time for epoch VLASS1.1
        super(NraoPages, self).__init__(config, start_dt)
        self._data_sources = []
        self._end_dt = None
        for url in config.data_sources:
            if 'quicklook' in url:
                self._data_sources.append(QuicklookPage(config, url, start_dt))
            else:
                self._data_sources.append(ContinuumImagingPage(config, url, start_dt))

    def __str__(self):
        msg = ''
        for data_source in self._data_sources:
            msg = f'{msg}\n{data_source.__class__.__name__}{data_source.__str__()}'
        return msg

    def _capture_todo(self, todo_count):
        self._reporter.capture_todo(todo_count, self._rejected_files, self._skipped_files)
        self._rejected_files = 0
        self._skipped_files = 0

    def initialize_end_dt(self):
        for data_source in self._data_sources:
            # initialize only once
            if len(data_source.todo_list) == 0:
                data_source.append_work()
                if len(data_source.todo_list) > 0:
                    # pick the earliest max_time among the data sources that change their max_time?
                    self._end_dt = (
                        data_source.end_dt if self._end_dt is None else min(self._end_dt, data_source.end_dt)
                    )

    def get_all_file_urls(self):
        """API for Validation.
        :returns dict with key: fully-qualified url to a file, value: datetime"""
        self._logger.debug('Begin get_all_file_urls')
        self.initialize_end_dt()
        result = {}
        for data_source in self._data_sources:
            # change a defaultdict(list) with key:datetime, value list[urls]
            # into a dict with key: url, value: datetime
            for dt, urls in data_source.todo_list.items():
                for url in urls:
                    result[url] = dt
        self._logger.debug('End get_all_file_urls')
        return result

    def get_time_box_work(self, prev_exec_dt, exec_dt):
        """
        Time-boxing the file url list returned from the site scrape.

        :param prev_exec_dt datetime start of the time-box chunk
        :param exec_dt datetime end of the time-box chunk
        :return: a list of StateRunnerMeta instances, for file names with
            time they were modified
        """
        self._logger.debug('Begin get_time_box_work')
        self.initialize_end_dt()
        self._logger.debug(self)

        temp = deque()
        for data_source in self._data_sources:
            for dt in data_source.todo_list:
                if prev_exec_dt < dt <= exec_dt:
                    for entry in data_source.todo_list[dt]:
                        temp.append(StateRunnerMeta(entry, dt))
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
        :param start_time datetime
        """
        for data_source in self._data_sources:
            data_source._start_dt = start_time


class VlassImagePage(HttpDataSource):

    def __init__(self, config, start_key, session):
        temp = '|'.join(f'{ii}$' for ii in config.data_source_extensions).replace('.', '\\.')

        def filter_by_extensions(href):
            return re.search(temp, href)

        # order: top page, tile/qa_rejected page, id (field centre) page, file listing page
        filter_functions = [filter_by_epoch_name, filter_by_tile, filter_by_epoch, filter_by_extensions]
        super().__init__(config, start_key, filter_functions, session)
        self._epochs = None
        self._session = session
        # override the HttpdDataSource._data_sources so that it does not treat all the NRAO image pages the same.
        self._data_sources = [start_key]

    def __str__(self):
        return (
            f'\n           start time:: {self.start_dt}'
            f'\n             end time:: {self.end_dt}'
            f'\nnumber of files found:: {len(self.todo_list)}'
            f'\n      number rejected:: {len(self.rejected)}'
        )


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
            epochs[key] = make_datetime(value)
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
                # https://archive-new.nrao.edu/vlass/weblog/se_calibration/
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
                                            dt = make_datetime(dt_str.strip())
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
                dt = make_datetime(dt_bits)
                if max_ts is None:
                    max_ts = dt
                    latest_key = key
                else:
                    if max_ts < dt:
                        max_ts = dt
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
