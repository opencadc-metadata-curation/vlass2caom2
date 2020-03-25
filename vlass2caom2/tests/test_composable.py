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
from vlass2caom2 import composable, VlassName, COLLECTION
import test_main_app
import test_scrape

# @patch('caom2pipe.execute_composable.CaomExecute._fits2caom2_cmd_local_direct')
# @patch('caom2pipe.execute_composable.CAOM2RepoClient')
# @patch('caom2pipe.execute_composable.CadcDataClient')
# def test_run_by_builder(data_client_mock, repo_mock, exec_mock):
#     repo_mock.return_value.read.side_effect = _mock_repo_read
#     repo_mock.return_value.create.side_effect = Mock()
#     repo_mock.return_value.update.side_effect = _mock_repo_update
#     data_client_mock.return_value.get_file_info.side_effect = \
#         _mock_get_file_info
#     getcwd_orig = os.getcwd
#     os.getcwd = Mock(return_value=TEST_DIR)
#     try:
#         # execution
#         sys.argv = ['test command']
#         test_result = composable._run_by_builder()
#         assert test_result == 0, 'wrong result'
#     finally:
#         os.getcwd = getcwd_orig
#
#     assert repo_mock.return_value.read.called, 'repo read not called'
#     assert repo_mock.return_value.create.called, 'repo create not called'
#     assert exec_mock.called, 'expect to be called'


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
    CadcTapClient.__init__ = Mock(return_value=None)

    test_obs_id = 'VLASS1.2.T07t13.J083838-153000'
    test_product_id = 'VLASS1.2.T07t13.J083838-153000.quicklook'
    test_f_name = 'VLASS1.2.ql.T07t13.J083838-153000.10.2048.v1.I.iter1.image.' \
                  'pbcor.tt0.subim.fits'
    try:
        # execution
        test_result = composable._run_state_rc()
        assert test_result == 0, 'mocking correct execution'
    finally:
        os.getcwd = getcwd_orig

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


def _mock_service_query():
    return None


def _mock_get_file_info():
    return None


def _mock_get_file():
    return None
