# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2021.                            (c) 2021.
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


from os.path import basename
from urllib.parse import urlparse
from caom2pipe import manage_composable as mc


__all__ = ['COLLECTION_PATTERN', 'QL_URL', 'SE_URL', 'VlassName']

COLLECTION_PATTERN = '*'  # TODO what are acceptable naming patterns?

QL_URL = 'https://archive-new.nrao.edu/vlass/quicklook/'
SE_URL = 'https://archive-new.nrao.edu/vlass/se_continuum_imaging/'


class VlassName(mc.StorageName):
    """Isolate the relationship between the observation id and the
    file names.

    Isolate the zipped/unzipped nature of the file names.

    While tempting, it's not possible to recreate URLs from file names,
    because some URLs are from the QA_REJECTED directories, hence
    the absence of that functionality in this class.

    SG - 03-02-21 - use the full fits filename plus _prev/_prev_256 for the preview/thumbnail file names
    """

    def __init__(
        self,
        entry=None,
    ):
        temp = urlparse(entry)
        if temp.scheme == '':
            file_name = basename(entry.replace('.header', ''))
        else:
            file_name = temp.path.split('/')[-1]
        super().__init__(file_name=file_name, source_names=[entry])
        self.set_root_url()
        self.set_version()

    @property
    def epoch(self):
        bits = self._file_name.split('.')
        return f'{bits[0]}.{bits[1]}'

    @property
    def prev(self):
        return f'{self._file_id}_prev.jpg'

    @property
    def rejected_url(self):
        return f'{self._root_url}{self.epoch}/QA_REJECTED/'

    @property
    def root_url(self):
        return self._root_url

    @property
    def thumb(self):
        return f'{self._file_id}_prev_256.jpg'

    @property
    def tile(self):
        return self._file_name.split('.')[3]

    @property
    def tile_url(self):
        return f'{self._root_url}{self.epoch}/{self.tile}/'

    @property
    def is_catalog(self):
        return '.catalog.' in self._file_name

    @property
    def is_channel_cube(self):
        return '.cc.' in self._file_name

    @property
    def is_quicklook(self):
        return '.ql.' in self._file_name

    def is_valid(self):
        return True

    @property
    def version(self):
        return self._version

    def is_qa_rejected(self):
        if self._source_names[0].startswith('http'):
            return 'QA_REJECTED' in self.source_names[0]
        else:
            raise mc.CadcException(f'Cannot tell REJECT status for {self}')

    def set_obs_id(self, **kwargs):
        """The obs id is made of the VLASS epoch, tile name, and image centre
        from the file name.
        """
        self._obs_id = VlassName.get_obs_id_from_file_name(self._file_name)

    def set_product_id(self, **kwargs):
        """The product id is made of the obs id plus the string 'quicklook'."""
        if self.is_quicklook:
            self._product_id = f'{self._obs_id}.quicklook'
        elif self.is_channel_cube:
            self._product_id = f'{self._obs_id}.channel_cube'
        else:
            self._product_id = f'{self._obs_id}.continuum_imaging'

    def set_root_url(self):
        self._root_url = SE_URL
        if self.is_quicklook:
            self._root_url = QL_URL

    def set_version(self):
        """The parameter may be a URI, or just the file name."""
        self._version = VlassName.extract_version(self._file_name)

    @staticmethod
    def extract_version(file_name):
        # fits file name looks like:
        # 'VLASS1.2.ql.T20t12.J092604+383000.10.2048.v2.I.iter1.image.pbcor.tt0.rms.subim.fits'
        #
        # csv file name looks like:
        # VLASS2.1.se.T11t35.J231002+033000.06.2048.v1.I.catalog.csv
        #
        # VLASS2.1.cc.T10t02.J005000-023000.06.2048.V_mean_minmaxrej.subim.fits.header
        result = 1
        if 'minmaxrej' not in file_name:
            bits = file_name.split('.')
            version_str = bits[7].replace('v', '')
            result = mc.to_int(version_str)
        return result

    @staticmethod
    def get_obs_id_from_file_name(file_name):
        """The obs id is made of the VLASS epoch, tile name, and image centre
        from the file name.
        """
        bits = file_name.split('.')
        return f'{bits[0]}.{bits[1]}.{bits[3]}.{bits[4]}'

    @staticmethod
    def remove_extensions(file_name):
        return file_name.replace('.fits', '').replace('.header', '')
