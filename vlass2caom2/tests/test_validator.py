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
from vlass2caom2 import validator

from mock import patch, Mock
import test_main_app, test_scrape


@patch('caom2pipe.manage_composable.repo_get')
@patch('cadcdata.core.net.BaseWsClient.post')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('caom2pipe.manage_composable.query_endpoint_session')
def test_validator(http_mock, caps_mock, post_mock, repo_get_mock):
    caps_mock.return_value = 'https://sc2.canfar.net/sc2repo'
    response = Mock()
    response.status_code = 200
    x = \
        [b'uri\n'
         b'ad:VLASS/VLASS1.1.ql.T01t01.J000228-363000.10.2048.v1.I.'
         b'iter1.image.pbcor.tt0.rms.subim.fits\n'
         b'ad:VLASS/VLASS1.1.ql.T01t01.J000228-363000.10.2048.v1.I.iter1.'
         b'image.pbcor.tt0.subim.fits\n'
         b'ad:VLASS/VLASS1.1.ql.T01t01.J000230-373000.10.2048.v1.I.iter1.'
         b'image.pbcor.tt0.rms.subim.fits']

    y = [b'ingestDate,fileName\n']

    global count
    count = 0

    def _mock_post(chunk_size):
        global count
        if count == 0:
            count = 1
            return x
        else:
            return y

    response.iter_content.side_effect = _mock_post
    post_mock.return_value.__enter__.return_value = response

    repo_get_mock.side_effect = _mock_repo_read

    if not os.path.exists('/root/.ssl/cadcproxy.pem'):
        if not os.path.exists('/root/.ssl'):
            os.mkdir('/root/.ssl')
        with open('/root/.ssl/cadcproxy.pem', 'w') as f:
            f.write('proxy content')

    http_mock.side_effect = test_scrape._query_endpoint

    getcwd_orig = os.getcwd
    os.getcwd = Mock(return_value=test_main_app.TEST_DATA_DIR)
    try:
        test_subject = validator.VlassValidator()
        test_listing_fqn = \
            f'{test_subject._config.working_directory}/{mc.VALIDATE_OUTPUT}'
        test_source_list_fqn = f'{test_subject._config.working_directory}/' \
                               f'{validator.NRAO_STATE}'
        if os.path.exists(test_listing_fqn):
            os.unlink(test_listing_fqn)
        if os.path.exists(test_subject._config.work_fqn):
            os.unlink(test_subject._config.work_fqn)
        if os.path.exists(test_source_list_fqn):
            os.unlink(test_source_list_fqn)

        test_source, test_meta, test_data = test_subject.validate()
        assert test_source is not None, 'expected source result'
        assert test_meta is not None, 'expected destination result'
        assert len(test_source) == 2, 'wrong number of source results'
        assert 'VLASS1.2.ql.T08t19.J123816-103000.10.2048.v2.I.iter1.image.' \
               'pbcor.tt0.rms.subim.fits' in test_source, \
            'wrong source content'
        assert len(test_meta) == 1, 'wrong # of destination results'
        assert 'VLASS1.1.ql.T01t01.J000230-373000.10.2048.v1.I.iter1.image.' \
               'pbcor.tt0.rms.subim.fits' in test_meta, \
            'wrong destination content'
        assert os.path.exists(test_listing_fqn), 'should create file record'

        test_subject.write_todo()
        assert os.path.exists(test_subject._config.work_fqn), \
            'should create file record'
        with open(test_subject._config.work_fqn, 'r') as f:
            content = f.readlines()
        assert len(content) == 2, 'wrong number of entries'
        compare = 'https://archive-new.nrao.edu/vlass/quicklook/VLASS1.2/' \
                  'QA_REJECTED/VLASS1.2.ql.T21t15.J141833+413000.10.2048.v1/' \
                  'VLASS1.2.ql.T08t19.J123816-103000.10.2048.v2.I.iter1.' \
                  'image.pbcor.tt0.rms.subim.fits\n'
        assert compare in content, 'unexpected content'

        # does the cached list work too?
        assert os.path.exists(test_source_list_fqn), 'cache should exist'
        test_cache = test_subject.read_from_source()
        assert test_cache is not None, 'expected cached source result'
        compare = 'VLASS1.2.ql.T08t19.J123816-103000.10.2048.v2.I.iter1.' \
                  'image.pbcor.tt0.rms.subim.fits'
        assert len(test_cache) == 4, 'wrong amount of cache content'
        assert compare in test_cache, 'wrong cached result'
    finally:
        os.getcwd = getcwd_orig


def test_multiple_versions():
    with open(f'{test_main_app.TEST_DATA_DIR}/multiple_versions_tile.html',
              'r') as f:
        test_string = f.read()
    test_start_date = datetime.strptime('2018-01-01', '%Y-%m-%d')
    start_content = scrape._parse_id_page(test_string, test_start_date)
    test_content = {}
    for key, value in start_content.items():
        test_key1 = f'https://archive-new.nrao.edu/vlass/quicklook/VLASS1.1/' \
                    f'T23t13{key}/{key.strip("/")}.I.iter1.image.pbcor.tt0.' \
                    f'subim.fits'
        test_key2 = f'https://archive-new.nrao.edu/vlass/quicklook/VLASS1.1/' \
                    f'T23t13{key}/{key.strip("/")}.I.iter1.image.pbcor.tt0.' \
                    f'rms.subim.fits'
        test_content[test_key1] = value.timestamp()
        test_content[test_key2] = value.timestamp()
    test_result, test_validate_dict_result = \
        validator.get_file_url_list_max_versions(test_content)
    assert test_result is not None, 'expect a test result'
    assert test_validate_dict_result is not None, 'expect a test result'
    assert len(test_result) == 82, 'wrong test result len'
    assert len(test_validate_dict_result) == 82, \
        'wrong test validate dict result len'

    for multiple in ['VLASS1.1.ql.T23t13.J120259+483000.10.2048',
                     'VLASS1.1.ql.T23t13.J125953+483000.10.2048']:
        l1 = f'{multiple}.v1.I.iter1.image.pbcor.tt0.subim.fits'
        l2 = f'{multiple}.v2.I.iter1.image.pbcor.tt0.subim.fits'
        assert l1 not in test_result.keys(), f'{l1} in test_result keys'
        assert l2 in test_result.keys(), f'{l2} not in test_result keys'
        assert l1 not in test_validate_dict_result.keys(), \
            f'{l1} in test_validate_dict_result keys'
        assert l2 in test_validate_dict_result.keys(), \
            f'{l2} not in test_validate_dict_result keys'


def _mock_repo_read(ignore_client, collection, obs_id, ignore_metrics):
    return SimpleObservation(obs_id, collection, Algorithm(name='exposure'))
