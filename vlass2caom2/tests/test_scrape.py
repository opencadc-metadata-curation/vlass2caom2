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
import sys

from datetime import datetime
from datetime import timedelta
from mock import patch, Mock, ANY

from caom2pipe import manage_composable as mc
from caom2.diff import get_differences

from vlass2caom2 import scrape, time_bounds_augmentation, composable
from vlass2caom2 import validator, VlassName, builder, data_source


THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TEST_DATA_DIR = os.path.join(THIS_DIR, 'data')
ALL_FIELDS = os.path.join(TEST_DATA_DIR, 'all_fields_list.html')
CAOM_QUERY = os.path.join(TEST_DATA_DIR, 'caom_query.csv')
CROSS_EPOCH = os.path.join(TEST_DATA_DIR, 'cross_epoch.html')
SINGLE_TILE = os.path.join(TEST_DATA_DIR, 'single_tile_list.html')
QL_INDEX = os.path.join(TEST_DATA_DIR, 'vlass_quicklook.html')
WL_INDEX = os.path.join(TEST_DATA_DIR, 'weblog_quicklook.html')
PIPELINE_INDEX = os.path.join(TEST_DATA_DIR, 'pipeline_weblog_quicklook.htm')
SINGLE_FIELD_DETAIL = os.path.join(TEST_DATA_DIR, 'single_field_detail.html')
REJECT_INDEX = os.path.join(TEST_DATA_DIR, 'rejected_index.html')
SPECIFIC_REJECTED = os.path.join(TEST_DATA_DIR, 'specific_rejected.html')
SPECIFIC_NO_FILES = os.path.join(TEST_DATA_DIR, 'no_files.html')
TEST_START_TIME_STR = '24Apr2019 12:34'
TEST_START_TIME = scrape.make_date_time(TEST_START_TIME_STR)
STATE_FILE = '/usr/src/app/state.yml'
TEST_OBS_ID = 'VLASS1.2.T07t14.J084202-123000'


class MyExitError(Exception):
    pass


# Response mock
class Object(object):
    pass

    def close(self):
        pass


def test_build_bits():
    with open(ALL_FIELDS) as f:
        test_content = f.read()
        test_result = scrape._parse_tile_page(test_content,
                                              TEST_START_TIME)
        assert test_result is not None, 'expected a result'
        assert len(test_result) == 4, 'wrong number of results'
        first_answer = next(iter(test_result.items()))
        assert len(first_answer) == 2, 'wrong number of results'
        assert first_answer[0] == 'T07t13/', 'wrong content'
        assert first_answer[1] == datetime(2019, 4, 29, 8, 2)

    with open(SINGLE_TILE) as f:
        test_content = f.read()
        test_result = scrape._parse_id_page(test_content,
                                            TEST_START_TIME)
        assert test_result is not None, 'expected a result'
        assert len(test_result) == 3, 'wrong number of results'
        first_answer = next(iter(test_result.items()))
        assert len(first_answer) == 2, 'wrong number of results'
        assert first_answer[0] == \
            'VLASS1.2.ql.T07t13.J080202-123000.10.2048.v1/'
        assert first_answer[1] == datetime(2019, 4, 26, 15, 19)

    with open(CROSS_EPOCH) as f:
        test_content = f.read()
        test_result = scrape._parse_id_page(test_content,
                                            TEST_START_TIME)
        assert test_result is not None, 'expected a result'
        assert len(test_result) == 5, 'wrong number of results'
        first_answer = next(iter(test_result.items()))
        assert len(first_answer) == 2, 'wrong number of results'
        assert first_answer[0] == \
               'VLASS1.1.ql.T05t33.J212207-203000.10.2048.v1/'
        assert first_answer[1] == datetime(2019, 7, 16, 9, 24)


@patch('caom2pipe.manage_composable.query_endpoint_session')
def test_build_todo_good(query_endpoint_mock):
    query_endpoint_mock.side_effect = _query_endpoint
    start_time = scrape.make_date_time('24Apr2019 12:34')
    test_result_list, test_result_date = scrape.build_good_todo(
        start_time, session=Mock()
    )
    assert test_result_list is not None, 'expected list result'
    assert test_result_date is not None, 'expected date result'
    assert len(test_result_list) == 3, 'wrong number of results'
    temp = test_result_list.popitem()
    assert temp[1][0] == \
        'https://archive-new.nrao.edu/vlass/quicklook/VLASS1.2/' \
        'T07t13/VLASS1.2.ql.T07t13.J083838-153000.10.2048.v1/', \
        temp[1][0]
    assert test_result_date == datetime(
        2019, 4, 28, 15, 18), 'wrong date result'


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
            'VLASS1.2_T07t14.J084202-123000_P35696v1_2019_03_11T23_06_' \
            '04.128/', 'wrong result'

    with open(SINGLE_FIELD_DETAIL) as f:
        test_content = f.read()
        test_result = scrape._parse_single_field(test_content)
        assert test_result is not None, 'expected a result'
        assert len(test_result) == 4, 'wrong number of fields'
        assert test_result['Pipeline Version'] == \
            '42270 (Pipeline-CASA54-P2-B)', 'wrong pipeline'
        assert test_result['Observation Start'] == \
            '2019-04-12 00:10:01', 'wrong start'
        assert test_result['Observation End'] == \
            '2019-04-12 00:39:18', 'wrong end'
        assert test_result['On Source'] == '0:03:54', 'wrong tos'


@patch('caom2pipe.manage_composable.query_endpoint_session')
def test_retrieve_qa_rejected(query_endpoint_mock):
    query_endpoint_mock.side_effect = _query_endpoint
    test_result_list, test_max_date = \
        scrape.build_qa_rejected_todo(
            TEST_START_TIME, session=Mock()
        )
    assert test_result_list is not None, 'expected dict result'
    assert len(test_result_list) == 1, 'wrong size results'
    temp = test_result_list.popitem()
    assert temp[1][0].startswith(
        'https://archive-new.nrao.edu/vlass/quicklook/VLASS1.2/'
        'QA_REJECTED/VLASS1.2.ql.T'), 'wrong reference'
    assert test_max_date is not None, 'expected date result'
    assert test_max_date == datetime(
        2019, 5, 1, 10, 30), 'wrong date result'


def test_qa_rejected_bits():
    with open(REJECT_INDEX) as f:
        test_content = f.read()
        test_result, test_max = scrape._parse_rejected_page(
            test_content, 'VLASS1.2', TEST_START_TIME,
            '{}VLASS1.2/QA_REJECTED/'.format(scrape.QL_URL))
        assert test_result is not None, 'expected a result'
        assert len(test_result) == 1, 'wrong number of results'
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


@patch('caom2pipe.manage_composable.query_endpoint_session')
def test_visit(query_endpoint_mock):
    test_id = 'VLASS1.2_T07t14.J084202-123000_P35696v1_2019_03_11T23_06_04.' \
              '128/'
    query_endpoint_mock.side_effect = _query_endpoint
    scrape.init_web_log_content({'VLASS1.2': TEST_START_TIME})
    scrape.web_log_content[test_id] = TEST_START_TIME - timedelta(hours=1)
    test_obs = mc.read_obs_from_file(
        '{}.xml'.format(os.path.join(TEST_DATA_DIR, TEST_OBS_ID)))
    kwargs = {'cadc_client': Mock()}
    test_result = time_bounds_augmentation.visit(test_obs, **kwargs)
    assert test_result is not None, 'expected a result'
    assert test_result['artifacts'] == 2, 'wrong result'

    obs_path = os.path.join(TEST_DATA_DIR, 'visit.xml')
    expected_obs = mc.read_obs_from_file(obs_path)
    result = get_differences(expected_obs, test_obs, 'Observation')
    if result:
        msg = 'Differences found in observation {}\n{}'. \
            format(TEST_OBS_ID, '\n'.join([r for r in result]))
        raise AssertionError(msg)


@patch('caom2pipe.manage_composable.query_endpoint_session')
def test_build_todo(query_endpoint_mock):
    query_endpoint_mock.side_effect = _query_endpoint
    test_result, test_max_date = scrape.build_todo(TEST_START_TIME)
    assert test_result is not None, 'expected dict result'
    assert len(test_result) == 4, 'wrong size results'
    assert test_max_date is not None, 'expected date result'
    assert test_max_date == datetime(
        2019, 4, 28, 15, 18), 'wrong date result'
    temp = test_result.popitem()
    assert temp[1][0] == \
        'https://archive-new.nrao.edu/vlass/quicklook/VLASS1.2/' \
        'QA_REJECTED/VLASS1.2.ql.T21t15.J141833+413000.10.2048.v1/', \
        'wrong result'


@patch('caom2pipe.manage_composable.query_endpoint_session')
def test_build_file_url_list(query_endpoint_mock):
    query_endpoint_mock.side_effect = _query_endpoint
    test_result, test_max_date = scrape.build_file_url_list(
        TEST_START_TIME)
    assert test_result is not None, 'expected dict result'
    assert len(test_result) == 4, 'wrong size results'
    assert test_max_date is not None, 'expected date result'
    assert test_max_date == datetime(
        2019, 4, 28, 15, 18), 'wrong date result'
    temp = test_result.popitem()
    assert temp[1][0] == \
        'https://archive-new.nrao.edu/vlass/quicklook/VLASS1.2/' \
        'QA_REJECTED/VLASS1.2.ql.T21t15.J141833+413000.10.2048.v1/' \
        'VLASS1.2.ql.T21t15.J141833+413000.10.2048.v1.I.iter1.image.' \
        'pbcor.tt0.rms.subim.fits', \
        temp[1][0]


@patch('caom2pipe.manage_composable.query_endpoint_session')
@patch('caom2pipe.run_composable.run_by_state')
def test_run_state_file_modify(run_mock, query_endpoint_mock):
    test_fname = 'VLASS1.2.ql.T21t15.J141833+413000.10.2048.v1.I.iter1.' \
                 'image.pbcor.tt0.subim.fits'
    # preconditions
    _write_state(TEST_START_TIME_STR)

    run_mock.side_effect = _run_mock
    getcwd_orig = os.getcwd
    os.getcwd = Mock(return_value=TEST_DATA_DIR)

    try:
        # execution
        query_endpoint_mock.side_effect = _query_endpoint
        composable._run_by_state()
        assert run_mock.assert_called, 'should have been called'
    finally:
        os.getcwd = getcwd_orig


@patch('vlass2caom2.scrape.requests.get')
def test_init_web_log_content(get_mock):
    get_mock.return_value.__enter__.return_value.raw = WL_INDEX
    if len(scrape.web_log_content) > 0:
        scrape.web_log_content = {}
    scrape.init_web_log_content({'VLASS1.2': TEST_START_TIME})
    assert scrape.web_log_content is not None, 'should be initialized'
    assert len(scrape.web_log_content) == 8, 'wrong record count'
    test_subject = scrape.web_log_content.popitem()
    assert isinstance(test_subject, tuple), \
        'wrong return type {}'.format(type(test_subject))
    assert test_subject[0] == \
        'VLASS1.2_T07t13.J083828-133000_P42512v1_2019_04_26T16_18_18.' \
        '714/', 'wrong first record'
    assert test_subject[1] == datetime(2019, 4, 28, 17, 37), \
        'wrong date result'


@patch('caom2pipe.manage_composable.query_endpoint_session')
def test_retrieve_metadata(query_endpoint_mock):
    query_endpoint_mock.side_effect = _query_endpoint
    test_result = scrape.retrieve_obs_metadata(
        'VLASS1.2.T07t13.J083453-133000')
    assert test_result is not None, 'expected dict result'
    assert len(test_result) == 5, 'wrong size results'
    assert test_result['reference'] == \
        'https://archive-new.nrao.edu/vlass/weblog/quicklook/' \
        'VLASS1.2_T07t13.J083453-133000_P42511v1_2019_04_26T16_17_56' \
        '.882/pipeline-20190422T202821/html/index.html', \
        'wrong reference'


@patch('caom2pipe.manage_composable.query_endpoint_session')
def test_read_list_from_nrao(query_endpoint_mock):
    nrao_file = os.path.join(TEST_DATA_DIR, 'nrao_state.csv')
    if os.path.exists(nrao_file):
        os.unlink(nrao_file)
    query_endpoint_mock.side_effect = _query_endpoint
    test_nrao, ignore = validator.read_list_from_nrao(nrao_file)
    assert test_nrao is not None, 'expected a nrao result'
    assert len(test_nrao) == 16, 'wrong nrao result'
    test_subject = test_nrao.popitem()
    assert test_subject[0].startswith('VLASS1.2.ql.T'), 'not a url'


@patch('caom2pipe.manage_composable.query_endpoint_session')
def test_list_files_on_page(query_endpoint_mock):
    result = Object()
    with open(f'{TEST_DATA_DIR}/file_list.html', 'r') as f:
        result.text = f.read()

    start_time = datetime(2018, 4, 15, 12, 34, 56)

    query_endpoint_mock.return_value = result

    test_list = scrape.list_files_on_page(
        'https://localhost:8080/', start_time, session=Mock()
    )
    assert test_list is not None, 'expect result'
    assert len(test_list) == 2, 'wrong number of results'
    assert 'https://archive-new.nrao.edu/vlass/quicklook/VLASS1.1/' \
           'T01t01/VLASS1.1.ql.T01t01.J000228-363000.10.2048.v1/' \
           'VLASS1.1.ql.T01t01.J000228-363000.10.2048.v1.I.iter1.' \
           'image.pbcor.tt0.subim.fits' in test_list, 'wrong content'


@patch('caom2pipe.execute_composable.OrganizeExecutes.do_one')
@patch('caom2pipe.manage_composable.query_endpoint_session')
def test_run_state(query_endpoint_mock, run_mock):
    _write_state('23Apr2019 10:30')
    # execution
    query_endpoint_mock.side_effect = _query_endpoint
    getcwd_orig = os.getcwd
    os.getcwd = Mock(return_value=TEST_DATA_DIR)
    try:
        sys.argv = ['test_command']
        composable._run_by_state()
        assert run_mock.called, 'should have been called'
        args, kwargs = run_mock.call_args
        test_storage = args[0]
        assert isinstance(test_storage, VlassName), type(test_storage)
        assert test_storage.url.startswith(
            'https://archive-new.nrao.edu/vlass/quicklook/VLASS1.2/'), \
            test_storage.url
        assert test_storage.url.endswith('.fits'), test_storage.url
        assert test_storage.file_name == test_storage.fname_on_disk, \
            'wrong fname on disk'
        assert run_mock.call_count == 40, 'wrong call count'
    finally:
        os.getcwd = getcwd_orig


@patch('caom2pipe.execute_composable.OrganizeExecutes.do_one')
@patch('caom2pipe.manage_composable.query_endpoint_session')
def test_run_state_with_work(query_endpoint_mock, run_mock):
    _write_state('23Apr2019 10:30')
    # execution

    def _run_mock_return(ignore1):
        return 0

    query_endpoint_mock.side_effect = _query_endpoint
    getcwd_orig = os.getcwd
    os.getcwd = Mock(return_value=TEST_DATA_DIR)
    try:
        # the first time through, the build_todo method will
        # use the MINIMUM of the good_date and the rejected_date,
        # because of the start times
        sys.argv = ['test_command']
        run_mock.side_effect = _run_mock_return
        test_result = composable._run_by_state()
        assert test_result is not None, 'expect a result'
        assert test_result == 0, 'wrong result'
        assert run_mock.called, 'should have been called'
        assert run_mock.call_count == 40, 'wrong call count'
        args, kwargs = run_mock.call_args
        test_storage = args[0]
        assert isinstance(test_storage, VlassName), type(test_storage)
        assert test_storage.url.startswith(
            'https://archive-new.nrao.edu/vlass/quicklook/VLASS1.2/'), \
            test_storage.url
        assert test_storage.url.endswith('.fits'), test_storage.url
        assert test_storage.file_name == test_storage.fname_on_disk, \
            'wrong fname'

        # the second time through, the build_todo method will
        # use the MAXIMUM of the good_date and the rejected_date,
        # because of the start times
        run_mock.reset_mock()
        assert not run_mock.called, 'reset worked'
        test_result = composable._run_by_state()
        assert test_result is not None, 'expect a result'
        assert test_result == 0, 'wrong test result'
        assert run_mock.called, 'run_mock not called'
        assert run_mock.call_count == 2, 'wrong number of calls'

        # and yes, this combination of start dates and comparison dates
        # will result in some records being processed more than once,
        # which is better than some records being missed

    finally:
        os.getcwd = getcwd_orig


@patch('caom2pipe.manage_composable.query_endpoint')
def test_query_by_tile_listing(query_mock):
    test_url = ''
    tile_list_content = mc.read_from_file(
        f'{TEST_DATA_DIR}/cleanup_augmentation_T20t12.html')
    result = Object()
    result.text = ''.join(ii for ii in tile_list_content)
    query_mock.return_value = result
    test_result = scrape.query_by_tile_listing(test_url)
    assert test_result is not None, 'expect result'
    assert len(test_result) == 38, 'wrong number of results'
    expected_url = 'https://archive-new.nrao.edu/vlass/quicklook/VLASS1.2/' \
                   'T20t12/VLASS1.2.ql.T20t12.J085530+373000.10.2048.v1/'
    assert expected_url in test_result, 'wrong results'

    qa_rejected_content = mc.read_from_file(
        f'{TEST_DATA_DIR}/cleanup_augmentation_QA_REJECTED.html')
    result.text = ''.join(ii for ii in qa_rejected_content)
    query_mock.return_value = result
    test_result = scrape.query_by_tile_listing(test_url)
    assert test_result is not None, 'expect result'
    assert len(test_result) == 85, 'wrong number of results'
    expected_url = 'https://archive-new.nrao.edu/vlass/quicklook/VLASS1.2/' \
                   'QA_REJECTED/VLASS1.2.ql.T01t46.J224731-373000.10.2048.v1/'
    assert expected_url in test_result, 'wrong results'


def _query_tap(ignore):
    with open(CAOM_QUERY) as f:
        temp = f.readlines()
    return [ii.strip() for ii in temp]


def _query_endpoint(url, session, timeout=-1):
    result = Object()
    result.text = None

    if url == 'https://archive-new.nrao.edu/vlass/quicklook/VLASS1.2/' \
                'T07t13/VLASS1.2.ql.T07t13.J080202-123000.10.2048.v1/':
        with open(f'{TEST_DATA_DIR}/file_list.html', 'r') as f:
            result.text = f.read()
    elif (url.startswith(
            'https://archive-new.nrao.edu/vlass/quicklook/VLASS1.2/') and
          url.endswith('.10.2048.v1/') and
          'QA_REJECTED' not in url):
        with open(SPECIFIC_NO_FILES) as f:
            result.text = f.read()
    elif url.endswith('index.html'):
        with open(SINGLE_FIELD_DETAIL) as f:
            result.text = f.read()
    elif url == scrape.QL_URL:
        with open(QL_INDEX) as f:
            result.text = f.read()
    elif 'vlass/quicklook/VLASS1.2/QA_REJECTED/VLASS1.2.ql' in url:
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
            if 'VLASS1.1' in url:
                result.text = ''
            else:
                with open(SINGLE_TILE) as f:
                    result.text = f.read()
    elif url.endswith('VLASS1.1/') or url.endswith('VLASS1.2/'):
        with open(ALL_FIELDS) as f:
            result.text = f.read()
    else:
        raise Exception('wut? {} {}'.format(url, len(url.split('/'))))
    return result


def _write_state(start_time_str):
    test_time = scrape.make_date_time(start_time_str)
    test_bookmark = {'bookmarks': {'vlass_timestamp':
                                        {'last_record': test_time}},
                     'context': {'vlass_context':
                                     {'VLASS1.1': '01-Jan-2018 00:00',
                                      'VLASS1.2': '01-Nov-2018 00:00',
                                      'VLASS2.1': '01-Jul-2020 00:00'}}}
    mc.write_as_yaml(test_bookmark, STATE_FILE)


def _run_mock(**kwargs):
    import logging
    logging.error('well, do I get here?')
    assert kwargs.get('command_name') == 'vlass2caom2'
    assert kwargs.get('end_time') == datetime(2019, 4, 28, 15, 18)
    test_config = kwargs.get('config')
    assert isinstance(test_config, mc.Config), type(test_config)
    assert test_config.work_fqn == '/usr/src/app/todo.txt', \
        'wrong todo file'
    assert test_config.state_fqn == '/usr/src/app/state.yml', \
        'wrong state file'
    test_builder = kwargs.get('name_builder')
    assert isinstance(test_builder, builder.VlassInstanceBuilder)
    test_source = kwargs.get('source')
    assert isinstance(test_source, data_source.NraoPage)
