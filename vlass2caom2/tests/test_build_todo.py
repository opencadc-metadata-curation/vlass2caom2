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

import os
import pytest
import sys

from datetime import datetime
from mock import patch, Mock

from caom2pipe import manage_composable as mc
from caom2.diff import get_differences

from vlass2caom2 import scrape, vlass_time_bounds_augmentation, composable

PY_VERSION = '3.6'
THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TEST_DATA_DIR = os.path.join(THIS_DIR, 'data')
ALL_FIELDS = os.path.join(TEST_DATA_DIR, 'all_fields_list.html')
SINGLE_FIELD = os.path.join(TEST_DATA_DIR, 'single_field_list.html')
QL_INDEX = os.path.join(TEST_DATA_DIR, 'vlass_quicklook.html')
WL_INDEX = os.path.join(TEST_DATA_DIR, 'weblog_quicklook.html')
PIPELINE_INDEX = os.path.join(TEST_DATA_DIR, 'pipeline_weblog_quicklook.htm')
SINGLE_FIELD_DETAIL = os.path.join(TEST_DATA_DIR, 'single_field_detail.html')
REJECT_INDEX = os.path.join(TEST_DATA_DIR, 'rejected_index.html')
SPECIFIC_REJECTED = os.path.join(TEST_DATA_DIR, 'specific_rejected.html')
TEST_START_TIME_STR = '24Apr2019 12:34'
TEST_START_TIME = datetime.strptime(TEST_START_TIME_STR,
                                    scrape.PAGE_TIME_FORMAT)
STATE_FILE = os.path.join(TEST_DATA_DIR, 'state.yml')
TEST_OBS_ID = 'VLASS1.2.T07t14.J084202-123000'


# Response mock
class Object(object):
    pass

    def close(self):
        pass


@pytest.mark.skipif(not sys.version.startswith(PY_VERSION),
                    reason='support single version')
def test_build_bits():
    with open(ALL_FIELDS) as f:
        test_content = f.read()
        test_result = scrape._parse_field_page(test_content,
                                               TEST_START_TIME)
        assert test_result is not None, 'expected a result'
        assert len(test_result) == 37, 'wrong number of results'
        first_answer = next(iter(test_result.items()))
        assert len(first_answer) == 2, 'wrong number of results'
        assert first_answer[0] == 'T07t13/', 'wrong content'
        assert first_answer[1] == datetime(2019, 4, 29, 8, 2)

    with open(SINGLE_FIELD) as f:
        test_content = f.read()
        test_result = scrape._parse_id_page(test_content,
                                            'VLASS1.2',
                                            TEST_START_TIME)
        assert test_result is not None, 'expected a result'
        assert len(test_result) == 8, 'wrong number of results'
        first_answer = next(iter(test_result.items()))
        assert len(first_answer) == 2, 'wrong number of results'
        assert first_answer[0] == \
            'VLASS1.2.ql.T07t13.J081013-123000.10.2048.v1/'
        assert first_answer[1] == datetime(2019, 4, 25, 8, 4)


@pytest.mark.skipif(not sys.version.startswith(PY_VERSION),
                    reason='support single version')
def test_build_todo_good():
    with patch('caom2pipe.manage_composable.query_endpoint') as \
            query_endpoint_mock:
        query_endpoint_mock.side_effect = _query_endpoint
        test_result_list, test_result_date = scrape.build_good_todo(
            TEST_START_TIME)
        assert test_result_list is not None, 'expected list result'
        assert test_result_date is not None, 'expected date result'
        assert len(test_result_list) == 6, 'wrong number of results'
        temp = test_result_list.popitem()
        assert temp[1][0].startswith(
            'https://archive-new.nrao.edu/vlass/quicklook/VLASS1.2/'
            'T07t13/VLASS1.2.ql.T07t13.J083453-133000.10.2048.v1/'), \
            'wrong type of list result'
        assert test_result_date == datetime(
            2019, 4, 29, 8, 2), 'wrong date result'


@pytest.mark.skipif(not sys.version.startswith(PY_VERSION),
                    reason='support single version')
def test_retrieve_metadata():
    with patch(
            'caom2pipe.manage_composable.query_endpoint') as query_endpoint_mock:
        query_endpoint_mock.side_effect = _query_endpoint
        test_result = scrape.retrieve_obs_metadata(TEST_OBS_ID)
        assert test_result is not None, 'expected dict result'
        assert len(test_result) == 5, 'wrong size results'
        assert test_result['reference'] == \
               'https://archive-new.nrao.edu/vlass/weblog/quicklook/' \
               'VLASS1.2_T07t14.J084202-123000_P35696v1_2019_03_11T23_06_04.128/' \
               'pipeline-20190422T202821/html/index.html', \
            'wrong reference'


@pytest.mark.skipif(not sys.version.startswith(PY_VERSION),
                    reason='support single version')
def test_augment_bits():
    with open(PIPELINE_INDEX) as f:
        test_content = f.read()
        test_result = scrape._parse_for_reference(test_content,
                                                  'pipeline-')
        assert test_result is not None, 'expected a result'
        assert test_result == 'pipeline-20190422T202821/', 'wrong result'

    with open(WL_INDEX) as f:
        test_content = f.read()
        test_result = scrape._parse_for_reference(test_content,
                                                  TEST_OBS_ID)
        assert test_result is not None, 'expected a result'
        assert test_result == \
               'VLASS1.2_T07t14.J084202-123000_P35696v1_2019_03_11T23_06_04.128/', \
            'wrong result'

    with open(SINGLE_FIELD_DETAIL) as f:
        test_content = f.read()
        test_result = scrape._parse_single_field(test_content)
        assert test_result is not None, 'expected a result'
        assert len(test_result) == 4, 'wrong number of fields'
        assert test_result[
                   'Pipeline Version'] == '42270 (Pipeline-CASA54-P2-B)', 'wrong pipline'
        assert test_result[
                   'Observation Start'] == '2019-04-12 00:10:01', 'wrong start'
        assert test_result[
                   'Observation End'] == '2019-04-12 00:39:18', 'wrong end'
        assert test_result['On Source'] == '0:03:54', 'wrong tos'


@pytest.mark.skipif(not sys.version.startswith(PY_VERSION),
                    reason='support single version')
def test_retrieve_qa_rejected():
    with patch(
            'caom2pipe.manage_composable.query_endpoint') as query_endpoint_mock:
        query_endpoint_mock.side_effect = _query_endpoint
        test_result_list, test_max_date = \
            scrape.build_qa_rejected_todo(TEST_START_TIME)
        assert test_result_list is not None, 'expected dict result'
        assert len(test_result_list) == 4, 'wrong size results'
        temp = test_result_list.popitem()
        assert temp[1][0].startswith(
            'https://archive-new.nrao.edu/vlass/quicklook/VLASS1.2/'
            'QA_REJECTED/VLASS1.2.ql.T'), 'wrong reference'
        assert test_max_date is not None, 'expected date result'
        assert test_max_date == datetime(
            2019, 5, 1, 10, 30), 'wrong date result'


@pytest.mark.skipif(not sys.version.startswith(PY_VERSION),
                    reason='support single version')
def test_qa_rejected_bits():
    with open(REJECT_INDEX) as f:
        test_content = f.read()
        test_result, test_max = scrape._parse_rejected_page(
            test_content, 'VLASS1.2', TEST_START_TIME,
            '{}VLASS1.2/QA_REJECTED/'.format(scrape.QL_URL))
        assert test_result is not None, 'expected a result'
        assert len(test_result) == 4, 'wrong number of results'
        temp = test_result.popitem()
        assert temp[1][0] == \
            'https://archive-new.nrao.edu/vlass/quicklook/VLASS1.2/' \
            'QA_REJECTED/VLASS1.2.ql.T21t15.J141833+413000.10.2048.v1/'
        assert test_max is not None, 'expected max result'
        assert test_max == datetime(
            2019, 5, 1, 10, 30), 'wrong date result'

    with open(SPECIFIC_REJECTED) as f:
        test_content = f.read()
        test_result = scrape._parse_specific_rejected_page(test_content)
        assert test_result is not None, 'expected a result'
        assert len(test_result) == 2, 'wrong result'
        assert 'VLASS1.2.ql.T08t19.J123816-103000.10.2048.v2.I.iter1.image.' \
               'pbcor.tt0.rms.subim.fits' in test_result, 'wrong content'


@pytest.mark.skipif(not sys.version.startswith(PY_VERSION),
                    reason='support single version')
def test_visit():
    with patch(
            'caom2pipe.manage_composable.query_endpoint') as query_endpoint_mock:
        query_endpoint_mock.side_effect = _query_endpoint
        test_obs = mc.read_obs_from_file(
            '{}.xml'.format(os.path.join(TEST_DATA_DIR, TEST_OBS_ID)))
        test_result = vlass_time_bounds_augmentation.visit(test_obs)
        assert test_result is not None, 'expected a result'
        assert test_result['artifacts'] == 2, 'wrong result'

        obs_path = os.path.join(TEST_DATA_DIR, 'visit.xml')
        expected_obs = mc.read_obs_from_file(obs_path)
        result = get_differences(expected_obs, test_obs, 'Observation')
        if result:
            msg = 'Differences found in observation {}\n{}'. \
                format(TEST_OBS_ID, '\n'.join([r for r in result]))
            raise AssertionError(msg)


@pytest.mark.skipif(not sys.version.startswith(PY_VERSION),
                    reason='support single version')
def test_build_todo():
    with patch(
            'caom2pipe.manage_composable.query_endpoint') as query_endpoint_mock:
        query_endpoint_mock.side_effect = _query_endpoint
        test_result, test_max_date = scrape.build_todo(TEST_START_TIME)
        assert test_result is not None, 'expected dict result'
        assert len(test_result) == 10, 'wrong size results'
        assert test_max_date is not None, 'expected date result'
        assert test_max_date == datetime(
            2019, 4, 29, 8, 2), 'wrong date result'
        temp = test_result.popitem()
        assert temp[1][0] == \
            'https://archive-new.nrao.edu/vlass/quicklook/VLASS1.2/' \
            'QA_REJECTED/VLASS1.2.ql.T21t15.J141833+413000.10.2048.v1/', \
            'wrong result'


@pytest.mark.skipif(not sys.version.startswith(PY_VERSION),
                    reason='support single version')
def test_run_state():
    # preconditions
    test_bookmark = {'bookmarks': {'vlass_timestamp':
                                    {'last_record': TEST_START_TIME}}}
    mc.write_as_yaml(test_bookmark, STATE_FILE)
    start_time = os.path.getmtime(STATE_FILE)

    getcwd_orig = os.getcwd
    os.getcwd = Mock(return_value=TEST_DATA_DIR)

    try:
        # execution
        with patch(
                'caom2pipe.manage_composable.query_endpoint') as \
                query_endpoint_mock, \
                patch('caom2pipe.execute_composable.run_single_from_state') \
                        as run_mock:
            query_endpoint_mock.side_effect = _query_endpoint
            composable.run_state()
            assert run_mock.called, 'should have been called'
        end_time = os.path.getmtime(STATE_FILE)
        assert end_time > start_time, 'no execution'
    finally:
        os.getcwd = getcwd_orig


def _query_endpoint(url):
    result = Object()
    result.text = None
    if url == scrape.QL_WEB_LOG_URL:
        with open(WL_INDEX) as f:
            result.text = f.read()
    elif url.endswith('index.html'):
        with open(SINGLE_FIELD_DETAIL) as f:
            result.text = f.read()
    elif url == scrape.QL_URL:
        with open(QL_INDEX) as f:
            result.text = f.read()
    elif 'vlass/quicklook/VLASS1.2//QA_REJECTED/VLASS1.2.ql' in url:
        with open(SPECIFIC_REJECTED) as f:
            result.text = f.read()
    elif 'QA_REJECTED' in url:
        with open(REJECT_INDEX) as f:
            result.text = f.read()
    elif len(url.split('/')) == 8:
        if 'weblog' in url:
            with open(PIPELINE_INDEX) as f:
                result.text = f.read()
        else:
            with open(SINGLE_FIELD) as f:
                result.text = f.read()
    elif url.endswith('VLASS1.1/') or url.endswith('VLASS1.2/'):
        with open(ALL_FIELDS) as f:
            result.text = f.read()
    else:
        raise Exception('wut? {}'.format(url))
    return result
