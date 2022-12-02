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
#  : 4 $
#
# ***********************************************************************
#

from caom2pipe.manage_composable import (
    Metrics, Observable, read_obs_from_file, Rejected, StorageName
)
from vlass2caom2 import preview_augmentation, cleanup_augmentation
from vlass2caom2.storage_name import VlassName

from mock import patch
from test_main_app import TEST_DATA_DIR


@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
def test_preview_augmentation(access_mock, test_config):
    orig_scheme = StorageName.scheme
    orig_collection = StorageName.collection
    try:
        StorageName.collection = test_config.collection
        StorageName.scheme = test_config.scheme
        access_mock.return_value = 'https://localhost'
        test_fqn = f'{TEST_DATA_DIR}/preview_augmentation_start.xml'
        test_science_f_name = 'VLASS1.1.ql.T01t01.J000228-363000.10.2048.v1.I.iter1.image.pbcor.' 'tt0.subim.fits'
        test_storage_name = VlassName(test_science_f_name)
        test_obs = read_obs_from_file(test_fqn)
        test_rejected = Rejected(f'{TEST_DATA_DIR}/rejected.yml')
        test_metrics = Metrics(test_config)
        test_observable = Observable(test_rejected, test_metrics)
        kwargs = {
            'stream': None,
            'observable': test_observable,
            'storage_name': test_storage_name,
            'working_directory': '/test_files',
        }
        test_subject = preview_augmentation.VlassPreview(**kwargs)
        assert test_subject is not None, 'need a test subject'
        assert len(test_obs.planes) == 1, 'wrong number of planes'
        assert len(test_obs.planes[test_storage_name.product_id].artifacts) == 4, 'wrong starting # of artifacts'
        test_obs = test_subject.visit(test_obs)
        assert test_obs is not None, 'expect a result'
        assert len(test_obs.planes[test_storage_name.product_id].artifacts) == 6, 'wrong ending # of artifacts'
        test_report = test_subject.report
        assert test_report is not None
        assert 'artifacts' in test_report
        assert test_report.get('artifacts') == 2, 'wrong report count'

        # does artifact re-naming work?
        test_url = (
            f'https://archive-new.nrao.edu/vlass/quicklook/VLASS1.1/'
            f'T01t01/VLASS1.1.ql.T01t01.J000228-363000.10.2048.v1/'
            f'{test_science_f_name}'
        )
        kwargs = {'url': test_url}
        test_obs = cleanup_augmentation.visit(test_obs, **kwargs)
        test_artifacts = test_obs.planes[test_storage_name.product_id].artifacts
        assert len(test_artifacts) == 4, 'wrong ending conditions'
        assert test_storage_name.prev_uri in test_artifacts, 'missing preview'
        assert test_storage_name.thumb_uri in test_artifacts, 'missing thumbnail'
    finally:
        StorageName.scheme = orig_scheme
        StorageName.collection = orig_collection
