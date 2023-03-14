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

from caom2pipe import manage_composable as mc
from vlass2caom2 import cleanup_augmentation
import test_main_app


def test_visit(test_config, tmp_path):
    test_config.change_working_directory(tmp_path.as_posix())
    test_product_id = 'VLASS1.2.T20t12.J092604+383000.quicklook'
    test_fname = f'{test_main_app.TEST_DATA_DIR}/visit_start.xml'
    test_obs = mc.read_obs_from_file(test_fname)

    test_artifacts = test_obs.planes[test_product_id].artifacts
    assert len(test_artifacts) == 4, 'wrong starting conditions'

    test_observable = mc.Observable(mc.Rejected(test_config.rejected_fqn), mc.Metrics(test_config))
    kwargs = {'observable': test_observable}
    test_obs = cleanup_augmentation.visit(test_obs, **kwargs)
    assert test_obs is not None, 'wrong return value'
    assert len(test_artifacts) == 2, 'wrong ending conditions'
    assert len(test_observable.rejected.content['old_version']) == 2, 'record capture failure'
    test_uri1 = 'nrao:VLASS/VLASS1.2.ql.T20t12.J092604+383000.10.2048.v2.I.iter1.image.pbcor.tt0.rms.subim.fits'
    test_uri2 = 'nrao:VLASS/VLASS1.2.ql.T20t12.J092604+383000.10.2048.v2.I.iter1.image.pbcor.tt0.subim.fits'
    assert test_uri1 in test_observable.rejected.content['old_version'], 'uri1 rejection tracking'
    assert test_uri2 in test_observable.rejected.content['old_version'], 'uri2 rejection tracking'


def test_clean_up_old_thumbnails_visit(test_config, tmp_path):
    test_config.change_working_directory(tmp_path.as_posix())
    test_product_id = 'VLASS1.1.T06t32.J211902-193000.quicklook'
    test_fname = f'{test_main_app.TEST_DATA_DIR}/too_many_artifacts.xml'
    test_obs = mc.read_obs_from_file(test_fname)

    test_artifacts = test_obs.planes[test_product_id].artifacts
    assert len(test_artifacts) == 6, 'wrong pre-conditions'

    test_observable = mc.Observable(mc.Rejected(test_config.rejected_fqn), mc.Metrics(test_config))
    kwargs = {'observable': test_observable}
    test_obs = cleanup_augmentation.visit(test_obs, **kwargs)
    assert test_obs is not None, 'wrong return value'
    assert len(test_artifacts) == 4, 'wrong post-conditions'
    test_uri1 = 'cadc:VLASS/VLASS1.1.ql.T06t32.J211902-193000.10.2048.v2.I.iter1.image.pbcor.tt0.subim_prev.jpg'
    test_uri2 = 'cadc:VLASS/VLASS1.1.ql.T06t32.J211902-193000.10.2048.v2.I.iter1.image.pbcor.tt0.subim_prev_256.jpg'
    test_uri3 = 'nrao:VLASS/VLASS1.1.ql.T06t32.J211902-193000.10.2048.v2.I.iter1.image.pbcor.tt0.subim.fits'
    test_uri4 = 'nrao:VLASS/VLASS1.1.ql.T06t32.J211902-193000.10.2048.v2.I.iter1.image.pbcor.tt0.rms.subim.fits'
    assert test_uri1 in test_obs.planes[test_product_id].artifacts.keys(), 'uri1'
    assert test_uri2 in test_obs.planes[test_product_id].artifacts.keys(), 'uri2'
    assert test_uri3 in test_obs.planes[test_product_id].artifacts.keys(), 'uri3'
    assert test_uri4 in test_obs.planes[test_product_id].artifacts.keys(), 'uri4'
