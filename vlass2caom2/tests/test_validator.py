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

from datetime import datetime

from caom2 import SimpleObservation, Algorithm
from caom2pipe import manage_composable as mc
from vlass2caom2 import composable, data_source, storage_name, validator

from mock import patch, Mock
import test_data_source
import test_main_app


@patch('caom2pipe.client_composable.repo_get')
@patch('cadcutils.net.BaseWsClient.post')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('vlass2caom2.data_source.query_endpoint_session')
def test_validator(http_mock, caps_mock, post_mock, repo_get_mock, test_config, tmp_path):
    caps_mock.return_value = 'https://sc2.canfar.net/sc2repo'
    response = Mock()
    response.status_code = 200

    # the first entry should be the last entry from the source list
    # the second entry is a made-up one to make sure the DATA -> SOURCE check finds something
    storage_answer = [
        b'uri\tcontentLastModified\n'
        b'nrao:VLASS/VLASS1.2.ql.T21t15.J141833+413000.10.2048.v1.I.iter1.image.pbcor.tt0.subim.fits'
        b'\t2017-12-12 00:00:00.000\n'
        b'nrao:VLASS/VLASS1.2.ql.T00t00.J141833+413000.10.2048.v1.I.iter1.image.pbcor.tt0.subim.fits'
        b'\t2017-12-12 00:00:00.000'
    ]

    response.iter_content.side_effect = [storage_answer]
    post_mock.return_value.__enter__.return_value = response
    repo_get_mock.side_effect = _mock_repo_read
    http_mock.side_effect = test_data_source._query_quicklook_endpoint

    orig_cwd = os.getcwd()
    try:
        os.chdir(tmp_path)
        test_config.change_working_directory(tmp_path.as_posix())
        test_config.proxy_file_name = 'cadcproxy.pem'
        test_config.logging_level = 'WARNING'
        test_config.data_sources = [storage_name.QL_URL]
        test_config.data_source_extensions = ['.fits']
        test_config.write_to_file(test_config)

        with open(test_config.proxy_fqn, 'w') as f:
            f.write('proxy content')
        # need the state.yml file for the DataSource specializations, timestamp gets over-ridden
        mc.State.write_bookmark(test_config.state_fqn, composable.VLASS_BOOKMARK, datetime.utcnow())

        test_subject = validator.VlassValidator()
        test_listing_fqn = f'{test_subject._config.working_directory}/not_at_cadc.txt'
        test_source_list_fqn = f'{test_subject._config.working_directory}/{validator.NRAO_STATE}'

        test_source_missing, test_data_missing, test_data_older = test_subject.validate()
        assert test_source_missing is not None, 'expected source result'
        assert test_data_missing is not None, 'expected destination result'
        # 176 is the number of entries returned by the mock doing tile count * file name count
        # 175 is the answer, because DATA claims to have one of those files
        assert len(test_source_missing) == 175, f'wrong number of source results {len(test_source_missing)}'
        assert (
            'VLASS1.2.ql.T21t15.J141833+413000.10.2048.v1.I.iter1.image.pbcor.tt0.rms.subim.fits'
            == test_source_missing.loc[174, 'f_name']
        ), f'wrong source content {test_source_missing.loc[174, "f_name"]}'
        assert len(test_data_missing) == 1, 'wrong # of destination results'
        assert (
            'VLASS1.2.ql.T00t00.J141833+413000.10.2048.v1.I.iter1.image.pbcor.tt0.subim.fits'
            == test_data_missing.loc[1, 'f_name']
        ), f'wrong destination content {test_data_missing.loc[1, "f_name"]}'
        assert len(test_data_older) == 1, 'wrong # of destination data results'
        assert (
            'VLASS1.2.ql.T21t15.J141833+413000.10.2048.v1.I.iter1.image.pbcor.tt0.subim.fits'
            == test_data_older.loc[0, 'f_name']
        ), f'wrong destination data content {test_data_older.loc[0, "f_name"]}'
        assert os.path.exists(test_listing_fqn), 'should create file record'
        test_source_fqn = f'{test_subject._config.working_directory}/not_at_NRAO.txt'
        test_newer_fqn = f'{test_subject._config.working_directory}/newer_at_NRAO.txt'
        assert os.path.exists(test_listing_fqn), 'should create file record'
        assert os.path.exists(test_source_fqn), 'should create source file record'
        assert os.path.exists(test_newer_fqn), 'should create newer source file record'

        test_subject.write_todo()
        store_fqn = test_subject._config.work_fqn.replace('todo', 'store_todo')
        ingest_fqn = test_subject._config.work_fqn.replace('todo', 'ingest_todo')
        assert os.path.exists(store_fqn), 'should create store file record'
        assert os.path.exists(ingest_fqn), 'should create ingest file record'

        with open(store_fqn, 'r') as f:
            content = f.readlines()
        assert len(content) == 175, 'wrong number of entries'
        compare = (
            'https://archive-new.nrao.edu/vlass/quicklook/VLASS1.2v2/QA_REJECTED/'
            'VLASS1.2.ql.T21t15.J141833+413000.10.2048.v1/'
            'VLASS1.2.ql.T21t15.J141833+413000.10.2048.v1.I.iter1.image.pbcor.tt0.rms.subim.fits\n'
        )
        assert compare in content, 'unexpected content'
        with open(ingest_fqn, 'r') as f:
            content = f.readlines()
        assert len(content) == 1, 'wrong number of ingest entries'
        compare = 'VLASS1.2.ql.T21t15.J141833+413000.10.2048.v1.I.iter1.image.pbcor.tt0.subim.fits\n'
        assert compare in content, 'unexpected ingest content'

        # does the cached list work too?
        assert os.path.exists(test_source_list_fqn), 'cache should exist'
        test_cache = test_subject.read_from_source()
        assert test_cache is not None, 'expected cached source result'
        compare = 'VLASS1.2.ql.T21t15.J141833+413000.10.2048.v1.I.iter1.image.pbcor.tt0.subim.fits'
        assert len(test_cache) == 176, 'wrong amount of cache content'
        assert compare == test_cache.loc[175, 'f_name'], 'wrong cached result'
    finally:
        os.chdir(orig_cwd)


def test_multiple_versions():
    with open(
        f'{test_main_app.TEST_DATA_DIR}/multiple_versions_tile.html', 'r'
    ) as f:
        test_string = f.read()
    test_start_date = data_source.QuicklookPage.make_date_time('2018-01-01 00:00')
    test_config = mc.Config()
    test_config.state_fqn = os.path.join(test_main_app.TEST_DATA_DIR, 'state.yml')
    test_subject = data_source.QuicklookPage(test_config, storage_name.QL_URL)
    test_subject._start_time = test_start_date
    start_content = test_subject._parse_id_page(test_string)
    test_content = {}
    for key, value in start_content.items():
        test_key1 = (
            f'https://archive-new.nrao.edu/vlass/quicklook/VLASS1.1/'
            f'T23t13{key}/{key.strip("/")}.I.iter1.image.pbcor.tt0.'
            f'subim.fits'
        )
        test_key2 = (
            f'https://archive-new.nrao.edu/vlass/quicklook/VLASS1.1/'
            f'T23t13{key}/{key.strip("/")}.I.iter1.image.pbcor.tt0.'
            f'rms.subim.fits'
        )
        test_content[test_key1] = value.timestamp()
        test_content[test_key2] = value.timestamp()
    (
        test_result,
        test_validate_dict_result,
    ) = validator.get_file_url_list_max_versions(test_content)
    assert test_result is not None, 'expect a test result'
    assert test_validate_dict_result is not None, 'expect a test result'
    assert len(test_result) == 82, 'wrong test result len'
    assert (
        len(test_validate_dict_result) == 82
    ), 'wrong test validate dict result len'

    for multiple in [
        'VLASS1.1.ql.T23t13.J120259+483000.10.2048',
        'VLASS1.1.ql.T23t13.J125953+483000.10.2048',
    ]:
        l1 = f'{multiple}.v1.I.iter1.image.pbcor.tt0.subim.fits'
        l2 = f'{multiple}.v2.I.iter1.image.pbcor.tt0.subim.fits'
        assert l1 not in test_result.keys(), f'{l1} in test_result keys'
        assert l2 in test_result.keys(), f'{l2} not in test_result keys'
        assert (
            l1 not in test_validate_dict_result.keys()
        ), f'{l1} in test_validate_dict_result keys'
        assert (
            l2 in test_validate_dict_result.keys()
        ), f'{l2} not in test_validate_dict_result keys'


def _mock_repo_read(ignore_client, collection, obs_id, ignore_metrics):
    return SimpleObservation(obs_id, collection, Algorithm(name='exposure'))
