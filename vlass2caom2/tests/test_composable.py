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

from mock import patch, Mock

from cadctap import CadcTapClient
from caom2pipe import manage_composable as mc
from caom2utils import get_gen_proc_arg_parser
from caom2 import SimpleObservation, Algorithm
from vlass2caom2 import composable, VlassName, COLLECTION, scrape
import test_main_app
import test_scrape


@patch('caom2pipe.execute_composable.CaomExecute._fits2caom2_cmd_direct')
@patch('caom2pipe.execute_composable.CAOM2RepoClient')
@patch('caom2pipe.execute_composable.CadcDataClient')
def test_run_by_builder(data_client_mock, repo_mock, exec_mock):
    repo_mock.return_value.read.side_effect = _mock_repo_read
    repo_mock.return_value.create.side_effect = Mock()
    repo_mock.return_value.update.side_effect = _mock_repo_update
    data_client_mock.return_value.get_file_info.side_effect = \
        _mock_get_file_info

    exec_mock.side_effect = _cmd_direct_mock

    getcwd_orig = os.getcwd
    os.getcwd = Mock(return_value=test_main_app.TEST_DATA_DIR)

    test_config = mc.Config()
    test_config.get_executors()

    test_f_name = 'VLASS1.2.ql.T07t13.J083838-153000.10.2048.v1.I.iter1.' \
                  'image.pbcor.tt0.subim.fits'
    with open(test_config.work_fqn, 'w') as f:
        f.write(f'{test_f_name}\n')

    # the equivalent of calling work.init_web_log()
    scrape.web_log_content['abc'] = 123

    try:
        # execution
        test_result = composable._run()
        assert test_result == 0, 'wrong result'
    finally:
        os.getcwd = getcwd_orig

    assert repo_mock.return_value.read.called, 'repo read not called'
    assert repo_mock.return_value.create.called, 'repo create not called'
    assert exec_mock.called, 'expect to be called'
    if os.path.exists(test_config.work_fqn):
        os.unlink(test_config.work_fqn)


@patch('caom2pipe.execute_composable.CadcDataClient')
@patch('caom2pipe.manage_composable.query_endpoint')
@patch('caom2pipe.execute_composable.OrganizeExecutesWithDoOne.do_one')
def test_run_state(run_mock, query_mock, data_client_mock):
    test_scrape._write_state('24Apr2019 12:34')
    query_mock.side_effect = test_scrape._query_endpoint
    run_mock.return_value = 0
    data_client_mock.return_value.get_file_info.side_effect = \
        _mock_get_file_info
    getcwd_orig = os.getcwd
    os.getcwd = Mock(return_value=test_main_app.TEST_DATA_DIR)
    orig_client = CadcTapClient.__init__
    CadcTapClient.__init__ = Mock(return_value=None)

    test_obs_id = 'VLASS1.2.T07t13.J083838-153000'
    test_product_id = 'VLASS1.2.T07t13.J083838-153000.quicklook'
    test_f_name = 'VLASS1.2.ql.T07t13.J083838-153000.10.2048.v1.I.iter1.' \
                  'image.pbcor.tt0.subim.fits'
    try:
        # execution
        test_result = composable._run_by_state()
        assert test_result == 0, 'mocking correct execution'

        # assert query_mock.called, 'service query not created'
        # assert builder_data_mock.return_value.get_file.called, \
        #     'get_file not called'
        assert run_mock.called, 'should have been called'
        args, kwargs = run_mock.call_args
        test_storage = args[0]
        assert isinstance(test_storage, VlassName), type(test_storage)
        assert test_storage.obs_id == test_obs_id, 'wrong obs id'
        assert test_storage.file_name == test_f_name, 'wrong file name'
        assert test_storage.fname_on_disk == test_f_name, 'wrong fname on disk'
        assert test_storage.url.startswith(
            'https://archive-new.nrao.edu/vlass/quicklook/VLASS'), \
            f'wrong url start format {test_storage.url}'
        assert test_storage.url.endswith('.fits'), \
            f'wrong url end format {test_storage.url}'
        assert test_storage.lineage == f'{test_product_id}/ad:{COLLECTION}/' \
                                       f'{test_f_name}', 'wrong lineage'
        assert test_storage.external_urls is None, 'wrong external urls'
    finally:
        os.getcwd = getcwd_orig
        CadcTapClient.__init__ = orig_client


@patch('vlass2caom2.to_caom2')
@patch('caom2pipe.manage_composable.query_endpoint')
@patch('caom2pipe.execute_composable.CAOM2RepoClient')
def test_run_state_rc(repo_client_mock, query_mock, to_caom2_mock):
    test_scrape._write_state('24Apr2019 12:34')
    query_mock.side_effect = test_scrape._query_endpoint
    repo_client_mock.return_value.read.return_value = None
    to_caom2_mock.side_effect = _write_obs_mock
    getcwd_orig = os.getcwd
    os.getcwd = Mock(return_value=test_main_app.TEST_DATA_DIR)
    try:
        test_result = composable._run_by_state()
        assert test_result is not None, 'expect result'
        assert test_result == 0, 'expect success'
        assert repo_client_mock.return_value.read.called, 'read called'
        assert to_caom2_mock.called, 'to_caom2 called'
        assert query_mock.called, 'what about you?'
        args, kwargs = query_mock.call_args
        assert args[0].startswith('https://archive-new.nrao.edu/'), \
            'should be a URL'
    finally:
        os.getcwd = getcwd_orig


def _cmd_direct_mock():
    from caom2 import SimpleObservation, Algorithm
    obs = SimpleObservation(observation_id='VLASS1.2.T07t13.J083838-153000',
                            collection=COLLECTION,
                            algorithm=Algorithm(name='testing'))
    mc.write_obs_to_file(
        obs, '/usr/src/app/logs/VLASS1.2.T07t13.J083838-153000.fits.xml')


def _mock_service_query():
    return None


def _mock_get_file_info(arg1, arg2):
    # arg2 is the file name
    return {'name': arg2}


def _mock_get_file():
    return None


def _mock_repo_read(arg1, arg2):
    return None


def _mock_repo_update():
    assert True


def _mock_get_cadc_headers(archive, file_id):
    import logging
    logging.error(f'\n\n\nmock get cadc headers\n\n\n')
    return {'md5sum': 'md5:abc123'}


def _mock_x(archive, file_id, b, fhead):
    import logging
    logging.error(f'{archive} {file_id} {fhead}')
    logging.error(f'\n\n\ncalled called called \n\n\n')
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
    extensions = \
        [e + delim for e in x.split(delim) if e.strip()]
    headers = [fits.Header.fromstring(e, sep='\n') for e in extensions]
    return headers


def _write_obs_mock():
    args = get_gen_proc_arg_parser().parse_args()
    obs = SimpleObservation(collection=args.observation[0],
                            observation_id=args.observation[1],
                            algorithm=Algorithm(name='exposure'))
    mc.write_obs_to_file(obs, args.out_obs_xml)
