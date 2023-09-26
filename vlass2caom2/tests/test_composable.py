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

import logging
import os
import traceback
import shutil

from collections import deque
from datetime import datetime, timedelta
from unittest.mock import ANY, patch, Mock, PropertyMock

from cadcutils import exceptions
from cadcdata import FileInfo
from caom2pipe.data_source_composable import StateRunnerMeta
from caom2pipe.astro_composable import make_headers_from_file
from caom2pipe import execute_composable as ec
from caom2pipe.manage_composable import (
    Config, make_datetime, Observable, read_obs_from_file, State, TaskType, write_obs_to_file
)
from caom2pipe import run_composable, transfer_composable
from caom2utils import get_gen_proc_arg_parser
from caom2 import SimpleObservation, Algorithm
from vlass2caom2 import composable, VlassName
from vlass2caom2.data_source import VlassPages
from vlass2caom2.storage_name import QL_URL, SE_URL
from vlass2caom2.tests.test_data_source import _write_state

import test_data_source


@patch('caom2pipe.client_composable.ClientCollection')
@patch('caom2pipe.execute_composable.OrganizeExecutes.do_one')
def test_run_by_builder(exec_mock, clients_mock, test_config, test_data_dir):
    # clients_mock - avoid initialization errors against real services
    exec_mock.return_value = 0

    getcwd_orig = os.getcwd
    os.getcwd = Mock(return_value=test_data_dir)

    test_config.get_executors()

    test_f_name = 'VLASS1.2.ql.T07t13.J083838-153000.10.2048.v1.I.iter1.image.pbcor.tt0.subim.fits'
    with open(test_config.work_fqn, 'w') as f:
        f.write(f'{test_f_name}\n')

    try:
        # execution
        test_result = composable._run()
        assert test_result == 0, 'wrong result'
    finally:
        os.getcwd = getcwd_orig
        if os.path.exists(test_config.work_fqn):
            os.unlink(test_config.work_fqn)

    assert exec_mock.called, 'expect to be called'
    args, kwargs = exec_mock.call_args
    arg_0 = args[0]
    assert isinstance(arg_0, VlassName), 'wrong parameter type'
    assert arg_0.obs_id == 'VLASS1.2.T07t13.J083838-153000', 'wrong obs id'
    assert arg_0.source_names[0] == test_f_name, 'wrong source name'
    assert (
        arg_0.destination_uris[0] == f'{test_config.scheme}:{test_config.collection}/{test_f_name}'
    ), 'wrong destination uri'


aa = deque(
    [
        StateRunnerMeta(
            'https://archive-new.nrao.edu/vlass/quicklook/VLASS1.1/T07t13/'
            'VLASS1.1.ql.T07t13.J083838-153000.10.2048.v1/'
            'VLASS1.1.ql.T07t13.J083838-153000.10.2048.v1.I.iter1.image.pbcor.tt0.subim.fits',
            datetime(2019, 4, 24, 12, 34),
        ),
        StateRunnerMeta(
            'https://archive-new.nrao.edu/vlass/quicklook/VLASS1.2/T07t13/'
            'VLASS1.2.ql.T07t13.J083838-153000.10.2048.v1/'
            'VLASS1.2.ql.T07t13.J083838-153000.10.2048.v1.I.iter1.image.pbcor.tt0.subim.fits',
            datetime(2019, 4, 24, 12, 34) - timedelta(seconds=2000),
        ),
        StateRunnerMeta(
            'https://archive-new.nrao.edu/vlass/quicklook/VLASS2.1/T07t13/'
            'VLASS2.1.ql.T07t13.J083838-153000.10.2048.v1/'
            'VLASS2.2.ql.T07t13.J083838-153000.10.2048.v1.I.iter1.image.pbcor.tt0.subim.fits',
            datetime(2019, 4, 25, 12, 34),
        ),
     ],
)
b = datetime(year=2019, month=4, day=25, hour=12, minute=34)


@patch('caom2pipe.html_data_source.HttpDataSource.end_dt', PropertyMock(return_value=b))
@patch('vlass2caom2.data_source.VlassPages.get_time_box_work')
@patch('caom2pipe.html_data_source.HttpDataSource._initialize_end_dt')
@patch('caom2pipe.client_composable.ClientCollection')
@patch('caom2pipe.execute_composable.OrganizeExecutes.do_one')
def test_run_state(run_mock, client_mock, init_end_dt_mock, get_work_mock, test_config, test_data_dir):

    # the test case where one URL has updated files, and one URL does not
    # QuicklookPage has new files, ContinuumPage has no updates

    get_work_mock.side_effect = [aa, deque(), deque()]
    run_mock.return_value = 0
    client_mock.data_client.info.side_effect = _mock_get_file_info
    getcwd_orig = os.getcwd
    os.getcwd = Mock(return_value=test_data_dir)

    test_obs_id = 'VLASS2.2.T07t13.J083838-153000'
    test_product_id = 'VLASS2.2.T07t13.J083838-153000.quicklook'
    test_f_name = 'VLASS2.2.ql.T07t13.J083838-153000.10.2048.v1.I.iter1.image.pbcor.tt0.subim.fits'
    state_fqn = os.path.join(test_data_dir, 'state.yml')
    try:
        # execution
        test_config.data_sources = [QL_URL]
        _write_state('24Apr2019 12:34', state_fqn, test_config)
        test_config, test_metadata_reader, test_sources, test_name_builder, ignore_clients = composable._common_init()
        test_metadata_reader._client = client_mock.data_client
        test_result = run_composable.run_by_state(
            config=test_config,
            meta_visitors=composable.META_VISITORS,
            data_visitors=composable.DATA_VISITORS,
            name_builder=test_name_builder,
            sources=test_sources,
            store_transfer=transfer_composable.HttpTransfer(),
            metadata_reader=test_metadata_reader,
            clients=client_mock,
        )
        assert test_result == 0, 'mocking correct execution'

        assert run_mock.called, 'should have been called'
        args, kwargs = run_mock.call_args
        test_storage = args[0]
        assert isinstance(test_storage, VlassName), type(test_storage)
        assert test_storage.obs_id == test_obs_id, 'wrong obs id'
        assert test_storage.product_id == test_product_id, 'wrong product id'
        assert test_storage.file_name == test_f_name, f'wrong file name {test_storage.file_name}'
        assert test_storage.source_names[0].startswith(
            'https://archive-new.nrao.edu/vlass/quicklook/VLASS'
        ), f'wrong url start format {test_storage.source_names[0]}'
        assert test_storage.source_names[0].endswith('.fits'), f'wrong url end format {test_storage.source_names[0]}'
        test_state_end = State(test_config.state_fqn, test_config.time_zone)
        test_end_bookmark = test_state_end.get_bookmark(QL_URL)
        assert (
            test_sources[0].end_dt == test_end_bookmark
        ), f'wrong end time max {test_sources[0].end_dt} end {test_end_bookmark}'
    except Exception as e:
        logging.error(e)
        logging.error(traceback.format_exc())
        raise e
    finally:
        os.getcwd = getcwd_orig


dd = datetime(2019, 4, 24, 13, 34)
c = deque(
    [
        StateRunnerMeta(
            'https://archive-new.nrao.edu/vlass/quicklook/VLASS1.1/'
            'T07t13/VLASS1.1.ql.T07t13.J083838-153000.10.2048.v1/'
            'VLASS1.1.ql.T07t13.J083838-153000.10.2048.v1.I.iter1.image.pbcor.tt0.subim.fits',
            dd
        ),
    ],
)
ee = datetime(2019, 4, 24, 12, 34)
end_dt_count = 0


@patch('vlass2caom2.data_source.VlassPages')
@patch('caom2pipe.client_composable.ClientCollection')
@patch('caom2pipe.execute_composable.OrganizeExecutes.do_one')
def test_run_state_as_composable(run_mock, client_mock, m, test_config, tmp_path):
    # differs from run_state as it relies on composable.py execution, instead of mimicking it

    class M(VlassPages):

        def _initialize_end_dt(self):
            global end_dt_count
            if end_dt_count == 0:
                end_dt_count = 1
                self._end_dt = dd
                self._work = c
            else:
                self._end_dt = ee
                self._work = deque()

    m.side_effect = [
        M(test_config, QL_URL, Mock(), Mock()),
        M(test_config, SE_URL, Mock(), Mock()),
    ]
    test_config.change_working_directory(tmp_path)
    test_config.data_sources = [QL_URL, SE_URL]
    test_config.logging_level = 'INFO'
    orig_dir = os.getcwd()
    try:
        os.chdir(tmp_path)
        Config.write_to_file(test_config)
        _write_state('24Apr2019 12:34', test_config.state_fqn, test_config)
        run_mock.return_value = 0
        client_mock.data_client.info.side_effect = _mock_get_file_info
        test_product_id = 'VLASS1.1.T07t13.J083838-153000.quicklook'
        test_f_name = 'VLASS1.1.ql.T07t13.J083838-153000.10.2048.v1.I.iter1.image.pbcor.tt0.subim.fits'
        test_result = composable._run_state()
        assert test_result == 0, 'mocking correct execution'
        assert run_mock.called, 'should have been called'
        args, kwargs = run_mock.call_args
        test_storage = args[0]
        assert isinstance(test_storage, VlassName), type(test_storage)
        assert test_storage.obs_id == 'VLASS1.1.T07t13.J083838-153000', 'wrong obs id'
        assert test_storage.product_id == test_product_id, 'wrong product id'
        assert test_storage.file_name == test_f_name, f'wrong file name {test_storage.file_name}'
        assert test_storage.source_names[0].startswith('https://archive-new.nrao.edu/vlass/quicklook/VLASS'), 'url'
        assert test_storage.source_names[0].endswith('.fits'), f'wrong url end format {test_storage.source_names[0]}'
        test_state_end = State(test_config.state_fqn, test_config.time_zone)
        test_end_bookmark = test_state_end.get_bookmark(QL_URL)
        assert dd == test_end_bookmark, f'QL wrong end time max {dd} end {test_end_bookmark}'
        test_se_end_bookmark = test_state_end.get_bookmark(SE_URL)
        assert test_se_end_bookmark == ee, f'SE end should be the same {ee} end {test_se_end_bookmark}'
    except Exception as e:
        logging.error(e)
        logging.error(traceback.format_exc())
        raise e
    finally:
        os.chdir(orig_dir)


zero_records_test_time = datetime(2019, 4, 27)


@patch('caom2pipe.client_composable.ClientCollection')
@patch('vlass2caom2.data_source.VlassPages.get_time_box_work')
@patch('vlass2caom2.data_source.VlassPages._initialize_end_dt')
@patch('caom2pipe.execute_composable.OrganizeExecutes.do_one')
@patch(
    'caom2pipe.html_data_source.HttpDataSource.end_dt', new_callable=PropertyMock(return_value=zero_records_test_time)
)
def test_run_state_zero_records(
    end_dt_mock, run_mock, init_end_mock, get_work_mock, client_mock, test_config, test_data_dir
):

    test_config.data_sources = [QL_URL]
    state_fqn = os.path.join(test_data_dir, 'state.yml')
    _write_state(zero_records_test_time, state_fqn, test_config)
    # no records returned
    get_work_mock.return_value = deque()
    run_mock.return_value = 0
    client_mock.data_client.info.side_effect = _mock_get_file_info
    getcwd_orig = os.getcwd
    os.getcwd = Mock(return_value=test_data_dir)

    try:
        # execution
        test_config, test_metadata_reader, test_sources, test_name_builder, ignore_clients = composable._common_init()
        test_metadata_reader._client = client_mock.data_client
        test_result = run_composable.run_by_state(
            config=test_config,
            meta_visitors=composable.META_VISITORS,
            data_visitors=composable.DATA_VISITORS,
            name_builder=test_name_builder,
            sources=test_sources,
            store_transfer=transfer_composable.HttpTransfer(),
            metadata_reader=test_metadata_reader,
            clients=client_mock,
        )
        assert test_result == 0, 'mocking correct execution'
        assert not run_mock.called, 'zero records, should not have been called'
        test_state_end = State(test_config.state_fqn, test_config.time_zone)
        test_end_bookmark = test_state_end.get_bookmark(QL_URL)
        assert (
            zero_records_test_time == test_end_bookmark
        ), f'wrong end time max {zero_records_test_time} end {test_end_bookmark}'
    except Exception as e:
        logging.error(traceback.format_exc())
        raise e
    finally:
        os.getcwd = getcwd_orig


# global variables for test_run_state_store_ingest mock control
header_read_count = 0
info_count = 0


@patch(
    'caom2pipe.html_data_source.HttpDataSource.end_dt', new_callable=PropertyMock(return_value=datetime(2020, 4, 19))
)
@patch('vlass2caom2.preview_augmentation.visit')
@patch('vlass2caom2.time_bounds_augmentation.visit')
@patch('caom2pipe.transfer_composable.HttpTransfer')
@patch('caom2pipe.html_data_source.query_endpoint_session')
@patch('caom2pipe.client_composable.ClientCollection')
def test_run_state_store_ingest(
    client_mock, query_mock, transferrer_mock, visit_mock, preview_mock, end_dt_mock, test_config, test_data_dir
):
    test_dir = f'{test_data_dir}/store_ingest_test'
    transferrer_mock.return_value.get.side_effect = _mock_retrieve_file
    client_mock.data_client.get_head.side_effect = _mock_headers_read
    client_mock.data_client.info.side_effect = lambda x: FileInfo(id=x, md5sum='abc')
    visit_mock.side_effect = _mock_visit
    preview_mock.side_effect = _mock_visit
    test_state_fqn = f'{test_dir}/state.yml'
    test_config.data_sources = [QL_URL]
    _write_state('22Apr2019 12:34', test_state_fqn, test_config)
    query_mock.side_effect = test_data_source._query_quicklook_endpoint
    client_mock.metadata_client.read.return_value = None
    getcwd_orig = os.getcwd
    os.getcwd = Mock(return_value=test_dir)
    try:
        test_config, test_metadata_reader, test_sources, test_name_builder, ignore_clients = composable._common_init()
        test_metadata_reader._client = client_mock.data_client
        test_result = run_composable.run_by_state(
            config=test_config,
            meta_visitors=composable.META_VISITORS,
            data_visitors=composable.DATA_VISITORS,
            name_builder=test_name_builder,
            sources=test_sources,
            store_transfer=transferrer_mock,
            metadata_reader=test_metadata_reader,
            clients=client_mock,
        )
        assert test_result is not None, 'expect result'
        assert test_result == -1, 'expect failure, because of the retries'
        assert client_mock.metadata_client.read.called, 'read called'
        # 3 => 2 files, 1 success + 1 failure + 1 retry
        assert client_mock.metadata_client.read.call_count == 3, 'read call count'
        assert query_mock.called, 'query endpoint session calls'
        assert query_mock.call_count == 15, 'wrong endpoint session call count'
        query_mock.assert_called_with(
            'https://archive-new.nrao.edu/vlass/quicklook/VLASS2.2/T26t15/'
            'VLASS1.2.ql.T07t13.J083838-153000.10.2048.v1/',
            ANY,
        ), 'query mock call args'
        # make sure data is not being written to CADC storage :)
        assert client_mock.data_client.put.called, 'put should be called'
        assert client_mock.data_client.put.call_count == 3, 'wrong number of puts'
        client_mock.data_client.put.assert_called_with(
            '/usr/src/app/vlass2caom2/vlass2caom2/tests/data/store_ingest_test/VLASS1.1.T01t01.J000228-363000',
            f'{test_config.scheme}:{test_config.collection}/VLASS1.1.ql.T01t01.J000228-363000.10.2048.v1.I.'
            f'iter1.image.pbcor.tt0.rms.subim.fits',
        )

        test_obs_output = read_obs_from_file(f'{test_dir}/logs/VLASS1.1.T01t01.J000228-363000.xml')
        found = False
        for plane in test_obs_output.planes.values():
            for artifact in plane.artifacts.values():
                assert artifact.content_checksum.uri == 'md5:abc', 'artifact metadata not updated'
                found = True
        assert found, 'should have found the correct md5sum'

        assert client_mock.data_client.get_head.called, 'get_head called'
        assert client_mock.data_client.get_head.call_count == 3, 'get_head call count'
        assert client_mock.data_client.info.called, 'info called'
        assert client_mock.data_client.info.call_count == 3, 'info call count'
    finally:
        os.getcwd = getcwd_orig
        test_rejected_fqn = f'{test_dir}/rejected.yml'
        test_log_dir = f'{test_dir}/logs'
        for entry in [test_state_fqn, test_rejected_fqn]:
            if os.path.exists(entry):
                os.unlink(entry)

        if os.path.exists(test_log_dir):
            with os.scandir(test_log_dir) as entries:
                for entry in entries:
                    os.unlink(entry)
            os.rmdir(test_log_dir)


def test_store(test_config):
    test_config.logging_level = 'ERROR'
    test_config.change_working_directory('/tmp')
    test_url = (
        'https://archive-new.nrao.edu/vlass/quicklook/VLASS2.1/'
        'T10t12/VLASS2.1.ql.T10t12.J073401-033000.10.2048.v1/'
        'VLASS2.1.ql.T10t12.J073401-033000.10.2048.v1.I.iter1.image.'
        'pbcor.tt0.rms.subim.fits'
    )
    test_storage_name = VlassName(test_url)
    transferrer = Mock()
    clients_mock = Mock()
    observable = Observable(test_config)
    test_metadata_reader = Mock()
    test_subject = ec.Store(
        test_config,
        observable,
        clients_mock,
        test_metadata_reader,
        transferrer,
    )
    test_subject.execute({'storage_name': test_storage_name})
    assert clients_mock.data_client.put.called, 'expect a call'
    clients_mock.data_client.put.assert_called_with(
        '/tmp/VLASS2.1.T10t12.J073401-033000',
        f'{test_config.scheme}:{test_config.collection}/VLASS2.1.ql.T10t12.J073401-033000.10.2048.v1.I.'
        f'iter1.image.pbcor.tt0.rms.subim.fits',
    ), 'wrong put args'
    assert transferrer.get.called, 'expect a transfer call'
    test_f_name = (
        'VLASS2.1.ql.T10t12.J073401-033000.10.2048.v1.I.iter1.'
        'image.pbcor.tt0.rms.subim.fits'
    )
    transferrer.get.assert_called_with(
        f'https://archive-new.nrao.edu/vlass/quicklook/VLASS2.1/T10t12/'
        f'VLASS2.1.ql.T10t12.J073401-033000.10.2048.v1/{test_f_name}',
        f'/tmp/VLASS2.1.T10t12.J073401-033000/{test_f_name}',
    ), 'wrong transferrer args'


@patch('vlass2caom2.preview_augmentation.visit')
@patch('vlass2caom2.position_bounds_augmentation.visit')
@patch('caom2utils.data_util.get_local_headers_from_fits')
def test_run_scrape_modify(headers_mock, footprint_mock, preview_mock, test_config, test_data_dir, tmp_path):
    footprint_mock.side_effect = _mock_visit
    preview_mock.side_effect = _mock_visit
    headers_mock.side_effect = make_headers_from_file
    getcwd_orig = os.getcwd()
    test_config.change_working_directory(tmp_path)
    test_config.task_types = [TaskType.SCRAPE, TaskType.MODIFY]
    test_config.use_local_files = True
    test_config.data_sources = ['/tmp']
    test_f_name = 'VLASS1.1.ql.T01t01.J000228-363000.10.2048.v1.I.iter1.image.pbcor.tt0.subim.fits'

    try:
        os.chdir(tmp_path)
        Config.write_to_file(test_config)
        _write_state('2202-01-01 01:01:01', test_config.state_fqn, test_config)
        shutil.copy(os.path.join(test_data_dir, f'{test_f_name}.header'), f'/tmp/{test_f_name}')

        # execution
        test_result = composable._run()
        assert test_result == 0, 'wrong result'
    finally:
        os.chdir(getcwd_orig)


@patch(
    'caom2pipe.html_data_source.HttpDataSource.end_dt', new_callable=PropertyMock(return_value=datetime(2023, 6, 5, 13, 54))
)
@patch('vlass2caom2.preview_augmentation.visit')
@patch('vlass2caom2.time_bounds_augmentation.visit')
@patch('caom2pipe.transfer_composable.HttpTransfer')
@patch('caom2pipe.html_data_source.query_endpoint_session')
@patch('caom2pipe.client_composable.ClientCollection')
def test_run_state_cross_timebox(
    client_mock,
    query_mock,
    transferrer_mock,
    visit_mock,
    preview_mock,
    end_dt_mock,
    test_config,
    test_data_dir,
    tmp_path,
):
    # 30 records found, 28 records processed - the records found cross a time-box
    transferrer_mock.return_value.get.side_effect = _mock_retrieve_file
    client_mock.data_client.get_head.side_effect = _mock_headers_read_1
    client_mock.data_client.info.side_effect = lambda x: FileInfo(id=x, md5sum='abc')
    visit_mock.side_effect = _mock_visit
    preview_mock.side_effect = _mock_visit
    test_config.data_sources = [SE_URL, QL_URL]
    test_config.interval = 360
    test_config.task_types = [TaskType.STORE, TaskType.INGEST, TaskType.MODIFY]
    test_config.log_to_file = True
    test_config.change_working_directory(tmp_path)
    State.write_bookmark(test_config.state_fqn, QL_URL, make_datetime('2023-06-03 3:33'))
    test_state = State(test_config.state_fqn, test_config.time_zone)
    test_state.add_bookmark(QL_URL, make_datetime('2023-06-03 3:33'))
    test_state.add_bookmark(SE_URL, make_datetime('2023-06-03 2:22'))
    test_state.write_bookmarks(test_config.state_fqn)
    query_mock.side_effect = _query_two_timeboxes_endpoint
    client_mock.metadata_client.read.return_value = None
    cwd_orig = os.getcwd()
    try:
        os.chdir(tmp_path)
        Config.write_to_file(test_config)
        test_config, test_metadata_reader, test_sources, test_name_builder, ignore_clients = composable._common_init()
        test_metadata_reader._client = client_mock.data_client
        test_result = run_composable.run_by_state(
            config=test_config,
            meta_visitors=composable.META_VISITORS,
            data_visitors=composable.DATA_VISITORS,
            name_builder=test_name_builder,
            sources=test_sources,
            store_transfer=transferrer_mock,
            metadata_reader=test_metadata_reader,
            clients=client_mock,
        )
        assert test_result is not None, 'expect result'
        assert test_result == 0, 'expect success'
        assert client_mock.metadata_client.read.called, 'read called'
        # 4 => 4 files, 4 successes
        assert client_mock.metadata_client.read.call_count == 4, 'read call count'
        assert query_mock.called, 'query endpoint session calls'
        # 9 = 1 * top page + 4 * epoch + 2 * tile + 2 * field
        assert query_mock.call_count == 9, f'wrong endpoint session call count {query_mock.call_count}'
        query_mock.assert_called_with(
            'https://archive-new.nrao.edu/vlass/quicklook/VLASS3.1/T32t02/'
            'VLASS3.1.ql.T32t02.J234412+853000.10.2048.v1/',
            ANY,
        ), 'query mock call args'
        # make sure data is not being written to CADC storage :)
        assert client_mock.data_client.put.called, 'put should be called'
        assert client_mock.data_client.put.call_count == 4, 'wrong number of puts'
        client_mock.data_client.put.assert_called_with(
            f'{tmp_path}/VLASS3.1.T32t02.J234412+853000',
            f'{test_config.scheme}:{test_config.collection}/'
            'VLASS3.1.ql.T32t02.J234412+853000.10.2048.v1.I.iter1.image.pbcor.tt0.subim.fits',
        )

        test_obs_output = read_obs_from_file(f'{tmp_path}/logs/VLASS3.1.T32t02.J234412+853000.xml')
        found = False
        for plane in test_obs_output.planes.values():
            for artifact in plane.artifacts.values():
                assert artifact.content_checksum.uri == 'md5:abc', 'artifact metadata not updated'
                found = True
        assert found, 'should have found the correct md5sum'

        assert client_mock.data_client.get_head.called, 'get_head called'
        assert client_mock.data_client.get_head.call_count == 4, 'get_head call count'
        assert client_mock.data_client.info.called, 'info called'
        assert client_mock.data_client.info.call_count == 4, 'info call count'
    finally:
        os.chdir(cwd_orig)
    assert False


def _mock_service_query():
    return None


def _mock_get_file_info(arg1, arg2):
    # arg2 is the file name
    return FileInfo(id=arg2, md5sum='abc')


def _mock_get_file_info_1(arg2):
    global info_count
    if info_count == 1:
        info_count = 0
        return _mock_get_file_info(None, arg2)
    else:
        info_count = 1
        return None


def _mock_get_file():
    return None


def _mock_repo_read(arg1, arg2):
    return None


def _mock_repo_update():
    assert True


def _mock_get_cadc_headers(archive, file_id):
    return {'md5sum': 'md5:abc123'}


def _mock_x(archive, file_id, b, fhead):
    from astropy.io import fits

    x = """SIMPLE  =                    T / Written by IDL:  Fri Oct  6 01:48:35 2017
BITPIX  =                  -32 / Bits per pixel
NAXIS   =                    2 / Number of dimensions
NAXIS1  =                 2048 /
NAXIS2  =                 2048 /
DATATYPE= 'REDUC   '           /Data type, SCIENCE/CALIB/REJECT/FOCUS/TEST
TYPE    = 'image  '
END
"""
    delim = '\nEND'
    extensions = [e + delim for e in x.split(delim) if e.strip()]
    headers = [fits.Header.fromstring(e, sep='\n') for e in extensions]
    return headers


def _write_obs_mock():
    args = get_gen_proc_arg_parser().parse_args()
    obs = SimpleObservation(
        collection=args.observation[0],
        observation_id=args.observation[1],
        algorithm=Algorithm(name='exposure'),
    )
    write_obs_to_file(obs, args.out_obs_xml)


def _mock_retrieve_file(ignore, local_fqn):
    with open(local_fqn, 'w') as f:
        f.write('test content')


def _mock_headers_read(ignore):
    global header_read_count
    if header_read_count == 1:
        header_read_count = 0
        return _mock_x(None, None, None, None)
    else:
        header_read_count = 1
        raise exceptions.UnexpectedException(f'{ignore} UnexpectedException')


def _mock_headers_read_1(ignore):
    return _mock_x(None, None, None, None)


def _mock_visit(obs, **kwargs):
    return obs


def _query_two_timeboxes_endpoint(url, session, timeout=-1):
    import logging
    logging.error(url)
    from os.path import join
    from test_main_app import TEST_DATA_DIR

    QL_INDEX = join(TEST_DATA_DIR, join('two_timebox_endpoint', 'top_page.html'))
    page_21 = join(TEST_DATA_DIR, join('two_timebox_endpoint', 'vlass_quicklook_VLASS2.1.html'))
    page_31 = join(TEST_DATA_DIR, join('two_timebox_endpoint', 'vlass_quicklook_VLASS3.1.html'))
    tile_21 = join(TEST_DATA_DIR, join('two_timebox_endpoint', 'tile_21.html'))
    tile_31 = join(TEST_DATA_DIR, join('two_timebox_endpoint', 'tile_31.html'))
    single_21 = join(TEST_DATA_DIR, join('two_timebox_endpoint', 'single_21.html'))
    single_31 = join(TEST_DATA_DIR, join('two_timebox_endpoint', 'single_31.html'))

    result = type('response', (), {})
    result.text = None
    result.close = lambda: None
    result.raise_for_status = lambda: None

    if (url == QL_URL):
        with open(QL_INDEX) as f:
            result.text = f.read()
    elif url == 'https://archive-new.nrao.edu/vlass/quicklook/VLASS2.1/':
        with open(page_21) as f:
            result.text = f.read()
    elif url == 'https://archive-new.nrao.edu/vlass/quicklook/VLASS3.1/':
        with open(page_31) as f:
            result.text = f.read()
    elif url == 'https://archive-new.nrao.edu/vlass/quicklook/VLASS2.1/T01t01/':
        with open(tile_21) as f:
            result.text = f.read()
    elif url == 'https://archive-new.nrao.edu/vlass/quicklook/VLASS3.1/T32t02/':
        with open(tile_31) as f:
            result.text = f.read()
    elif url.startswith(
        'https://archive-new.nrao.edu/vlass/quicklook/VLASS2.1/T01t01/VLASS2.1.ql.T01t01.J000228-363000.10.2048.v1/'
    ):
        with open(single_21) as f:
            result.text = f.read()
    elif url.startswith(
        'https://archive-new.nrao.edu/vlass/quicklook/VLASS3.1/T32t02/VLASS3.1.ql.T32t02.J234412+853000.10.2048.v1/'
    ):
        with open(single_31) as f:
            result.text = f.read()
    else:
        result.text = '<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN"><html></html>'
    return result
