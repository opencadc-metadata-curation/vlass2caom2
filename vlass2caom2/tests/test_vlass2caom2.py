# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2018.                            (c) 2018.
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


from caom2pipe import manage_composable as mc
from vlass2caom2 import main_app, VlassName
from caom2.diff import get_differences

import os
import pytest
import sys

from mock import patch


TEST_URI = 'ad:TEST_COLLECTION/test_file.fits'

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TESTDATA_DIR = os.path.join(THIS_DIR, 'data')
PLUGIN = os.path.join(os.path.dirname(THIS_DIR), 'vlass2caom2.py')
a = 'VLASS1.1.ql.T01t01.J000228-363000.10.2048.' \
    'v1.I.iter1.image.pbcor.tt0.rms.subim.fits.header'
b = 'VLASS1.1.ql.T01t01.J000228-363000.10.2048.' \
    'v1.I.iter1.image.pbcor.tt0.subim.fits.header'
c = 'VLASS1.1.ql.T10t12.J075402-033000.10.2048.' \
    'v1.I.iter1.image.pbcor.tt0.rms.subim.fits.header'
d = 'VLASS1.1.ql.T10t12.J075402-033000.10.2048.' \
    'v1.I.iter1.image.pbcor.tt0.subim.fits.header'
e = 'VLASS1.1.ql.T29t05.J110448+763000.10.2048.' \
    'v1.I.iter1.image.pbcor.tt0.rms.subim.fits.header'
f = 'VLASS1.1.ql.T29t05.J110448+763000.10.2048.' \
    'v1.I.iter1.image.pbcor.tt0.subim.fits.header'
g = 'VLASS1.1.cat.T29t05.J110448+763000.10.2048.v1.csv'
h = 'VLASS1.1.cc.T29t05.J110448+763000.10.2048.v1.fits.header'
obs_id_a = 'VLASS1.1.T01t01.J000228-363000'
obs_id_c = 'VLASS1.1.T10t12.J075402-033000'
obs_id_e = 'VLASS1.1.T29t05.J110448+763000'

COLLECTION = 'VLASS'

features = mc.Features()
features.supports_catalog = False
if features.supports_catalog:
    test_obs = [[obs_id_a, a, b],
                [obs_id_c, c, d],
                [obs_id_e, e, f, g, h]]
else:
    test_obs = [[obs_id_a, a, b],
                [obs_id_c, c, d]]


@pytest.mark.parametrize('test_files', test_obs)
def test_main_app(test_files):
    obs_id = test_files[0]
    lineage = _get_lineage(obs_id, test_files)
    import logging
    logging.error(lineage)

    output_file = '{}.actual.xml'.format(obs_id)
    local = _get_local(test_files[1:])
    plugin = PLUGIN

    with patch('caom2utils.fits2caom2.CadcDataClient') as data_client_mock:
        def get_file_info(archive, file_id):
            if file_id == a:
                return {'size': 55425600,
                        'md5sum': 'ae2a33238c5051611133e7090560fd8a',
                        'type': 'application/fits'}
            else:
                return {'size': 55425600,
                        'md5sum': '40f7c2763f92ea6e9c6b0304c569097e',
                        'type': 'application/fits'}
        data_client_mock.return_value.get_file_info.side_effect = \
            get_file_info

        sys.argv = \
            ('vlass2caom2 --local {} --observation {} {} -o {} '
             '--plugin {} --module {} --lineage {}'.format(
                local, COLLECTION, obs_id, output_file, plugin, plugin,
                lineage)).split()
        print(sys.argv)
        main_app()
        obs_path = os.path.join(TESTDATA_DIR, '{}.xml'.format(obs_id))
        expected = mc.read_obs_from_file(obs_path)
        actual = mc.read_obs_from_file(output_file)
        result = get_differences(expected, actual, 'Observation')
        if result:
            msg = 'Differences found in observation {}\n{}'. \
                format(expected.observation_id, '\n'.join(
                    [r for r in result]))
            raise AssertionError(msg)
        # assert False  # cause I want to see logging messages


def _get_local(test_files):
    result = ''
    for test_name in test_files:
        result = '{} {}/{}'.format(result, TESTDATA_DIR, test_name)
    return result


def _get_lineage(obs_id, test_files):
    if obs_id_a is obs_id or obs_id_c is obs_id:
        product_id = '{}.quicklook.v1'.format(obs_id)
        return ' '.join(VlassName(obs_id, ii).get_lineage(product_id)
                        for ii in test_files[1:])
    else:
        ql_pid = '{}.quicklook.v1'.format(obs_id)
        cat_pid = '{}.catalog.v1'.format(obs_id)
        coarse_pid = '{}.coarsecube.v1'.format(obs_id)
        return '{}/ad:VLASS/{} {}/ad:VLASS/{} {}/ad:VLASS/{} ' \
               '{}/ad:VLASS/{}'.format(ql_pid, test_files[1], ql_pid,
                                       test_files[2], cat_pid, test_files[3],
                                       coarse_pid, test_files[4])
