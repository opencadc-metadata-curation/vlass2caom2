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

from urllib.parse import urlparse
from caom2pipe import caom_composable as cc
from caom2pipe import manage_composable as mc
from vlass2caom2 import scrape


__all__ = [
    'APPLICATION', 'COLLECTION', 'COLLECTION_PATTERN', 'SCHEME', 'VlassName'
]
COLLECTION = 'VLASS'
APPLICATION = 'vlass2caom2'
SCHEME = 'nrao'
CADC_SCHEME = 'cadc'
COLLECTION_PATTERN = '*'  # TODO what are acceptable naming patterns?


class VlassName(mc.StorageName):
    """Isolate the relationship between the observation id and the
    file names.

    Isolate the zipped/unzipped nature of the file names.

    While tempting, it's not possible to recreate URLs from file names,
    because some of the URLs are from the QA_REJECTED directories, hence
    the absence of that functionality in this class.
    """

    def __init__(
        self,
        entry=None,
    ):
        self._collection = COLLECTION
        self._entry = entry
        temp = urlparse(entry)
        if temp.scheme == '':
            self._url = None
            self._file_name = entry
        else:
            if temp.scheme.startswith('http'):
                self._url = entry
                self._file_name = temp.path.split('/')[-1]
            else:
                # it's an Artifact URI
                self._url = None
                self._file_name = temp.path.split('/')[-1]
        self._obs_id = VlassName.get_obs_id_from_file_name(self._file_name)
        self._product_id = VlassName.get_product_id_from_file_name(
            self._file_name
        )
        self._file_id = VlassName.remove_extensions(self._file_name)
        self._version = VlassName.get_version(self._file_name)
        self._scheme = SCHEME
        self._destination_uris = [self.file_uri]

    def __str__(self):
        return (
            f'\n'
            f'      obs_id: {self.obs_id}\n'
            f'     file_id: {self.file_id}\n'
            f'   file_name: {self.file_name}\n'
            f'source_names: {self.source_names}\n'
            f'    file_uri: {self.file_uri}\n'
            f'     lineage: {self.lineage}\n'
            f'         url: {self.url}\n'
        )

    @property
    def archive(self):
        return self._archive

    @property
    def epoch(self):
        bits = self._file_name.split('.')
        return f'{bits[0]}.{bits[1]}'

    @property
    def file_id(self):
        return self._file_id

    @property
    def file_uri(self):
        """No .gz extension, unlike the default implementation."""
        return self._get_uri(self._file_name, SCHEME)

    @property
    def file_name(self):
        return self._file_name

    @property
    def image_pointing_url(self):
        bits = self._file_name.split('.')
        return f'{self.tile_url}{self.epoch}.ql.{self.tile}.{bits[4]}.' \
               f'{bits[5]}.{bits[6]}.{bits[7]}/'

    @property
    def prev(self):
        return f'{self._file_id}_prev.jpg'

    @property
    def prev_uri(self):
        return self._get_uri(self.prev, CADC_SCHEME)

    @property
    def product_id(self):
        return self._product_id

    @property
    def rejected_url(self):
        return f'{scrape.QL_URL}{self.epoch}/QA_REJECTED/'

    @property
    def scheme(self):
        return self._scheme

    @property
    def source_names(self):
        return [self.entry]

    @property
    def thumb(self):
        return f'{self._file_id}_prev_256.jpg'

    @property
    def thumb_uri(self):
        return self._get_uri(self.thumb, CADC_SCHEME)

    @property
    def tile(self):
        return self._file_name.split('.')[3]

    @property
    def tile_url(self):
        return f'{scrape.QL_URL}{self.epoch}/{self.tile}/'

    @property
    def url(self):
        return self._url

    def is_valid(self):
        return True

    @property
    def version(self):
        return self._version

    def _get_uri(self, file_name, scheme):
        return cc.build_artifact_uri(file_name, self._collection, scheme)

    @staticmethod
    def get_obs_id_from_file_name(file_name):
        """The obs id is made of the VLASS epoch, tile name, and image centre
        from the file name.
        """
        bits = file_name.split('.')
        obs_id = f'{bits[0]}.{bits[1]}.{bits[3]}.{bits[4]}'
        return obs_id

    @staticmethod
    def get_product_id_from_file_name(file_name):
        """The product id is made of the obs id plus the string 'quicklook'."""
        obs_id = VlassName.get_obs_id_from_file_name(file_name)
        return f'{obs_id}.quicklook'

    @staticmethod
    def get_version(entry):
        """The parameter may be a URI, or just the file name."""
        # file name looks like:
        # 'VLASS1.2.ql.T20t12.J092604+383000.10.2048.v2.I.iter1.image.
        #                'pbcor.tt0.rms.subim.fits'
        file_name = entry
        if '/' in entry:
            file_name = mc.CaomName(entry).file_name
        bits = file_name.split('.')
        version_str = bits[7].replace('v', '')
        return mc.to_int(version_str)

    @staticmethod
    def remove_extensions(file_name):
        return file_name.replace('.fits', '').replace('.header', '')
