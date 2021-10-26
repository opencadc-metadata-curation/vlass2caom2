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

from caom2pipe import name_builder_composable as nbc
from vlass2caom2 import storage_name as sn


def test_storage_name():
    test_bit = (
        'VLASS1.2.ql.T23t09.J083851+483000.10.2048.v1.I.iter1.image.pbcor.tt0'
    )
    test_url = (
        f'https://archive-new.nrao.edu/vlass/quicklook/VLASS1.2v2/T23t09/'
        f'VLASS1.2.ql.T23t09.J083851+483000.10.2048.v1/{test_bit}.subim.fits'
    )
    ts1 = sn.VlassName(test_url)
    ts2 = sn.VlassName(f'{test_bit}.subim.fits')
    for ts in [ts1, ts2]:
        assert ts.obs_id == 'VLASS1.2.T23t09.J083851+483000', 'wrong obs id'
        assert (
            ts.product_id == 'VLASS1.2.T23t09.J083851+483000.quicklook'
        ), 'wrong product id'
        assert ts.file_name == f'{test_bit}.subim.fits', 'wrong fname'
        assert ts.file_id == f'{test_bit}.subim', 'wrong fid'
        assert (
            ts.file_uri == f'{sn.SCHEME}:VLASS/{test_bit}.subim.fits'
        ), 'wrong uri'
        assert (
            ts.model_file_name == 'VLASS1.2.T23t09.J083851+483000.xml'
        ), 'wrong model name'
        assert (
            ts.log_file == 'VLASS1.2.T23t09.J083851+483000.log'
        ), 'wrong log file'
        assert (
            sn.VlassName.remove_extensions(ts.file_name) == f'{test_bit}.subim'
        ), 'wrong extensions'
        assert ts.epoch == 'VLASS1.2', 'wrong epoch'
        assert (
            ts.tile_url == 'https://archive-new.nrao.edu/vlass/quicklook/'
            'VLASS1.2/T23t09/'
        ), 'wrong tile url'
        assert (
            ts.rejected_url == 'https://archive-new.nrao.edu/vlass/'
            'quicklook/VLASS1.2/QA_REJECTED/'
        ), 'wrong rejected url'
        assert (
            ts.image_pointing_url == 'https://archive-new.nrao.edu/vlass/'
            'quicklook/VLASS1.2/T23t09/VLASS1.2.ql.'
            'T23t09.J083851+483000.10.2048.v1/'
        ), 'wrong image pointing url'
        assert ts.prev == f'{test_bit}.subim_prev.jpg', 'wrong preview'
        assert ts.thumb == f'{test_bit}.subim_prev_256.jpg', 'wrong thumbnail'
        assert (
            ts.prev_uri == f'{sn.CADC_SCHEME}:{sn.COLLECTION}/'
                           f'{test_bit}.subim_prev.jpg'
        ), 'wrong preview uri'
        assert (
            ts.thumb_uri == f'{sn.CADC_SCHEME}:{sn.COLLECTION}/'
                            f'{test_bit}.subim_prev_256.jpg'
        ), 'wrong thumbnail uri'
        assert (
            ts.lineage
            == f'{ts.product_id}/{sn.SCHEME}:{sn.COLLECTION}/'
               f'{test_bit}.subim.fits'
        ), 'wrong lineage'


def test_source_names():
    test_url = (
        'https://archive-new.nrao.edu/vlass/quicklook/VLASS1.2/T23t09/'
        'VLASS1.2.ql.T23t09.J083851+483000.10.2048.v1/VLASS1.2.ql.T23t09.'
        'J083851+483000.10.2048.v1.I.iter1.image.pbcor.tt0.subim.fits'
    )
    test_f_name = (
        'VLASS1.2.ql.T23t09.J083851+483000.10.2048.v1.I.iter1.image.pbcor.'
        'tt0.subim.fits'
    )
    test_subject = nbc.EntryBuilder(sn.VlassName)
    test_result = test_subject.build(test_url)
    assert len(test_result.source_names) == 1, 'wrong length'
    assert test_result.source_names[0] == test_url, 'wrong result'

    test_result = test_subject.build(test_f_name)
    assert len(test_result.source_names) == 1, 'wrong length'
    assert test_result.source_names[0] == test_f_name, 'wrong result'
