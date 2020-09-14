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

import os
import pytest
import shutil

from mock import Mock

from caom2 import Status
from caom2pipe import manage_composable as mc

from vlass2caom2 import time_bounds_augmentation, quality_augmentation
from vlass2caom2 import position_bounds_augmentation

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TEST_DATA_DIR = os.path.join(THIS_DIR, 'data')
TEST_URI = 'ad:VLASS/VLASS1.1.ql.T01t01.J000228-363000.10.2048.v1.I.iter1.' \
           'image.pbcor.tt0.rms.subim.fits'


def test_aug_visit():
    with pytest.raises(mc.CadcException):
        time_bounds_augmentation.visit(None)
    with pytest.raises(mc.CadcException):
        quality_augmentation.visit(None)


def test_aug_visit_works():
    test_file = os.path.join(
        TEST_DATA_DIR, 'VLASS1.1.T01t01.J000228-363000.xml')
    test_obs = mc.read_obs_from_file(test_file)
    assert test_obs is not None, 'unexpected None'

    data_dir = os.path.join(THIS_DIR, '../../data')
    kwargs = {'working_directory': data_dir,
              'cadc_client': Mock()}
    test_result = time_bounds_augmentation.visit(test_obs, **kwargs)
    assert test_obs is not None, 'unexpected modification'
    assert test_result is not None, 'should have a result status'
    assert len(test_result) == 1, 'modified artifacts count'
    assert test_result['artifacts'] == 2, 'artifact count'
    plane = test_obs.planes['VLASS1.1.T01t01.J000228-363000.quicklook']
    chunk = plane.artifacts[TEST_URI].parts['0'].chunks[0]
    assert chunk is not None
    assert chunk.time is not None, 'no time information'
    assert chunk.time.axis is not None, 'no axis information'
    assert chunk.time.axis.bounds is not None, 'no bounds information'
    assert len(chunk.time.axis.bounds.samples) == 1, \
        'wrong amount of bounds info'
    assert chunk.time.exposure == 401.0, \
        'wrong exposure value'
    mc.write_obs_to_file(test_obs, os.path.join(TEST_DATA_DIR, 'x.xml'))


def test_aug_visit_quality_works():
    rejected_uri = 'ad:VLASS/VLASS1.1.ql.T10t12.J075402-033000.10.2048.v1' \
                   '.I.iter1.image.pbcor.tt0.subim.fits'
    test_file = os.path.join(
        TEST_DATA_DIR, 'VLASS1.1.T01t01.J000228-363000.xml')
    test_obs = mc.read_obs_from_file(test_file)
    assert test_obs is not None, 'unexpected None'

    data_dir = os.path.join(THIS_DIR, '../../data')
    kwargs = {'working_directory': data_dir}
    test_result = quality_augmentation.visit(test_obs, **kwargs)
    assert test_obs is not None, 'unexpected modification'
    assert test_result is not None, 'should have a result status'
    assert len(test_result) == 1, 'modified artifacts count'
    assert test_result['observations'] == 0, 'observation count'
    assert test_obs.requirements is None, 'status value should not be set'

    for plane in test_obs.planes:
        for artifact in test_obs.planes[plane].artifacts:
            test_obs.planes[plane].artifacts[artifact].uri = rejected_uri
    test_result = quality_augmentation.visit(test_obs, **kwargs)
    assert test_obs is not None, 'unexpected modification'
    assert test_result is not None, 'should have a result status'
    assert len(test_result) == 1, 'modified artifacts count'
    assert test_result['observations'] == 2, 'observation count'
    assert test_obs.requirements.flag == Status.FAIL, 'wrong status value'

    mc.write_obs_to_file(test_obs, os.path.join(TEST_DATA_DIR, 'x.xml'))


def test_aug_visit_quality_works_uri():
    rejected_uri = 'https://archive-new.nrao.edu/QA_REJECTED'
    test_file = os.path.join(
        TEST_DATA_DIR, 'VLASS1.1.T01t01.J000228-363000.xml')
    test_obs = mc.read_obs_from_file(test_file)
    assert test_obs is not None, 'unexpected None'

    kwargs = {'uri': rejected_uri}
    for plane in test_obs.planes:
        for artifact in test_obs.planes[plane].artifacts:
            test_obs.planes[plane].artifacts[artifact].uri = rejected_uri
    test_result = quality_augmentation.visit(test_obs, **kwargs)
    assert test_obs is not None, 'unexpected modification'
    assert test_result is not None, 'should have a result status'
    assert len(test_result) == 1, 'modified artifacts count'
    assert test_result['observations'] == 2, 'observation count'
    assert test_obs.requirements.flag == Status.FAIL, 'wrong status value'

    mc.write_obs_to_file(test_obs, os.path.join(TEST_DATA_DIR, 'x.xml'))


def test_aug_visit_position_bounds():
    test_file_id = 'VLASS1.2.ql.T24t07.J065836+563000.10.2048.v1.I.' \
                   'iter1.image.pbcor.tt0.subim'
    test_input_file = f'/test_files/{test_file_id}.fits'
    if not os.path.exists(test_input_file):
        shutil.copy(f'/usr/src/app/test_files/{test_file_id}.fits',
                    test_input_file)
    test_file = os.path.join(TEST_DATA_DIR, 'fpf_start_obs.xml')
    test_obs = mc.read_obs_from_file(test_file)
    kwargs = {'working_directory': '/test_files',
              'science_file': f'{test_file_id}.fits',
              'log_file_directory': os.path.join(TEST_DATA_DIR, 'logs')}
    test_result = position_bounds_augmentation.visit(test_obs, **kwargs)
    assert test_result is not None, 'should have a result status'
    assert len(test_result) == 1, 'modified chunks count'
    assert test_result['chunks'] == 2, 'chunk count'
    return_file = os.path.join(THIS_DIR, '{test_file_id}__footprint.txt')
    assert not os.path.exists(return_file), 'bad cleanup'
    if os.path.exists(test_input_file):
        os.unlink(test_input_file)
