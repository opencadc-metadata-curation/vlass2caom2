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

import os

from datetime import datetime, timedelta, timezone
from dateutil import tz
from unittest.mock import ANY, patch, Mock

from cadcutils import exceptions
from cadcdata import FileInfo
from caom2pipe import execute_composable as ec
from caom2pipe.manage_composable import (
    Config, Metrics, Observable, read_obs_from_file, Rejected, StorageName, write_obs_to_file
)
from caom2pipe import run_composable, transfer_composable
from caom2utils import get_gen_proc_arg_parser
from caom2 import SimpleObservation, Algorithm
from vlass2caom2 import composable, VLASS_BOOKMARK, VlassName
import test_data_source
import test_main_app
from vlass2caom2.tests.test_data_source import _write_state

STATE_FILE = os.path.join(test_main_app.TEST_DATA_DIR, 'state.yml')


@patch('caom2pipe.client_composable.ClientCollection')
@patch('caom2pipe.execute_composable.OrganizeExecutes.do_one')
def test_run_by_builder(exec_mock, clients_mock, test_config):
    # clients_mock - avoid initialization errors against real services
    exec_mock.return_value = 0

    getcwd_orig = os.getcwd
    os.getcwd = Mock(return_value=test_main_app.TEST_DATA_DIR)

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
    assert arg_0.destination_uris[0] == f'{test_config.scheme}:{test_config.collection}/{test_f_name}', 'wrong destination uri'


@patch('caom2pipe.client_composable.ClientCollection')
@patch('vlass2caom2.data_source.QuicklookPage._build_todo')
@patch('caom2pipe.execute_composable.OrganizeExecutes.do_one')
def test_run_state(run_mock, query_mock, client_mock):

    def _mock_append_work():
        a = {
            datetime(2019, 4, 24, 12, 34, tzinfo=timezone.utc): [
                'https://archive-new.nrao.edu/vlass/quicklook/VLASS1.1/'
                'T07t13/VLASS1.1.ql.T07t13.J083838-153000.10.2048.v1/',
            ],
            datetime(2019, 4, 24, 12, 34, tzinfo=timezone.utc) - timedelta(seconds=2000): [
                'https://archive-new.nrao.edu/vlass/quicklook/VLASS1.2/'
                'T07t13/VLASS1.2.ql.T07t13.J083838-153000.10.2048.v1/',
            ],
            datetime(2019, 4, 25, 12, 34, tzinfo=timezone.utc): [
                'https://archive-new.nrao.edu/vlass/quicklook/VLASS2.1/'
                'T07t13/VLASS2.1.ql.T07t13.J083838-153000.10.2048.v1/',
            ],
        }
        b = datetime(
            year=2019,
            month=4,
            day=25,
            hour=12,
            minute=34,
            second=0,
            tzinfo=timezone.utc,
        )
        return a, b

    _write_state('24Apr2019 12:34', STATE_FILE)
    query_mock.side_effect = _mock_append_work
    run_mock.return_value = 0
    client_mock.data_client.info.side_effect = _mock_get_file_info
    getcwd_orig = os.getcwd
    os.getcwd = Mock(return_value=test_main_app.TEST_DATA_DIR)

    test_obs_id = 'VLASS2.1.T07t13.J083838-153000'
    test_product_id = 'VLASS2.1.T07t13.J083838-153000.quicklook'
    test_f_name = 'VLASS2.1.ql.T07t13.J083838-153000.10.2048.v1.I.iter1.image.pbcor.tt0.subim.fits'
    try:
        # execution
        test_config, test_metadata_reader, test_source, test_name_builder, ignore_clients = composable._common_init()
        test_source._max_time = datetime(2019, 4, 27, tzinfo=tz.gettz('US/Mountain'))
        test_metadata_reader._client = client_mock.data_client
        test_result = run_composable.run_by_state(
            config=test_config,
            bookmark_name=VLASS_BOOKMARK,
            meta_visitors=composable.META_VISITORS,
            data_visitors=composable.DATA_VISITORS,
            name_builder=test_name_builder,
            source=test_source,
            end_time=test_source.max_time,
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
        ), f'wrong url start format {test_storage.url}'
        assert test_storage.source_names[0].endswith(
            '.fits'
        ), f'wrong url end format {test_storage.url}'
    finally:
        os.getcwd = getcwd_orig


# global variables for test_run_state_store_ingest mock control
header_read_count = 0
info_count = 0


@patch('vlass2caom2.time_bounds_augmentation.visit')
@patch('caom2pipe.transfer_composable.HttpTransfer')
@patch('vlass2caom2.data_source.query_endpoint_session')
@patch('caom2pipe.client_composable.ClientCollection')
def test_run_state_store_ingest(client_mock, query_mock, transferrer_mock, visit_mock):
    test_dir = f'{test_main_app.TEST_DATA_DIR}/store_ingest_test'
    transferrer_mock.return_value.get.side_effect = _mock_retrieve_file
    client_mock.data_client.get_head.side_effect = _mock_headers_read
    client_mock.data_client.info.side_effect = _mock_get_file_info_1
    visit_mock.side_effect = _mock_visit
    test_state_fqn = f'{test_dir}/state.yml'
    _write_state('28Apr2019 12:34', test_state_fqn)
    query_mock.side_effect = test_data_source._query_quicklook_endpoint
    client_mock.metadata_client.read.return_value = None
    getcwd_orig = os.getcwd
    os.getcwd = Mock(return_value=test_dir)
    try:
        test_config, test_metadata_reader, test_source, test_name_builder, ignore_clients = composable._common_init()
        test_source._max_time = datetime(2019, 5, 2, tzinfo=tz.gettz('US/Mountain'))
        test_metadata_reader._client = client_mock.data_client
        test_result = run_composable.run_by_state(
            config=test_config,
            bookmark_name=VLASS_BOOKMARK,
            meta_visitors=composable.META_VISITORS,
            data_visitors=composable.DATA_VISITORS,
            name_builder=test_name_builder,
            source=test_source,
            end_time=test_source.max_time,
            store_transfer=transferrer_mock,
            metadata_reader=test_metadata_reader,
            clients=client_mock,
        )
        assert test_result is not None, 'expect result'
        assert test_result == 0, 'expect success'
        assert client_mock.metadata_client.read.called, 'read called'
        assert client_mock.metadata_client.read.call_count == 20, 'read call count'
        assert query_mock.called, 'query endpoint session calls'
        assert query_mock.call_count == 21, 'wrong endpoint session call count'
        query_mock.assert_called_with(
            'https://archive-new.nrao.edu/vlass/quicklook/VLASS2.2/QA_REJECTED/', ANY
        ), 'query mock call args'
        # make sure data is not being written to CADC storage :)
        assert client_mock.data_client.put.called, 'put should be called'
        assert client_mock.data_client.put.call_count == 20, 'wrong number of puts'
        client_mock.data_client.put.assert_called_with(
            '/usr/src/app/vlass2caom2/vlass2caom2/tests/data/store_ingest_test/VLASS1.2.T21t15.J141833+413000',
            f'{test_config.scheme}:{test_config.collection}/VLASS1.2.ql.T21t15.J141833+413000.10.2048.v1.I.'
            f'iter1.image.pbcor.tt0.subim.fits',
        )

        test_obs_output = read_obs_from_file(f'{test_dir}/logs/VLASS1.2.T07t13.J083838-153000.xml')
        for plane in test_obs_output.planes.values():
            for artifact in plane.artifacts.values():
                assert artifact.content_checksum.uri == 'md5:abc', 'artifact metadata not updated'

        assert client_mock.data_client.get_head.called, 'get_head called'
        # there are four unique file names in this test, so the 8 is for the failure the first time around, and the
        # success the second time around - using the DelayedClientReader in the inheritance tree
        assert client_mock.data_client.get_head.call_count == 8, 'get_head call count'
        assert client_mock.data_client.info.called, 'info called'
        assert client_mock.data_client.info.call_count == 8, 'info call count'
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
    test_config.working_directory = '/tmp'
    test_url = (
        'https://archive-new.nrao.edu/vlass/quicklook/VLASS2.1/'
        'T10t12/VLASS2.1.ql.T10t12.J073401-033000.10.2048.v1/'
        'VLASS2.1.ql.T10t12.J073401-033000.10.2048.v1.I.iter1.image.'
        'pbcor.tt0.rms.subim.fits'
    )
    test_storage_name = VlassName(test_url)
    transferrer = Mock()
    clients_mock = Mock()
    observable = Observable(
        Rejected('/tmp/rejected.yml'), Metrics(test_config)
    )
    test_metadata_reader = Mock()
    test_subject = ec.Store(
        test_config,
        observable,
        transferrer,
        clients_mock,
        test_metadata_reader,
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


def _mock_visit(
    obs,
    working_directory=None,
    clients=None,
    stream=None,
    storage_name=None,
    metadata_reader=None,
    observable=None,
):
    return obs
