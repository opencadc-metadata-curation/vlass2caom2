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

import importlib
import logging
import sys
import traceback

from math import sqrt

from caom2 import Observation, ProductType
from caom2utils import ObsBlueprint, get_gen_proc_arg_parser, gen_proc
from caom2pipe import StorageName
from caom2pipe import astro_composable as ac
from caom2pipe import manage_composable as mc
from vlass2caom2 import scrape


__all__ = ['vlass_main', 'update', 'VlassName', 'VlassCardinality',
           'COLLECTION', 'APPLICATION', 'to_caom2']

COLLECTION = 'VLASS'
APPLICATION = 'vlass2caom2'
COLLECTION_PATTERN = '*'  # TODO what are acceptable naming patterns?


class VlassName(StorageName):
    """Isolate the relationship between the observation id and the
    file names.

    Isolate the zipped/unzipped nature of the file names.

    While tempting, it's not possible to recreate URLs from file names,
    because some of the URLs are from the QA_REJECTED directories, hence
    the absence of that functionality in this class.
    """
    def __init__(self, obs_id=None, file_name=None, fname_on_disk=None,
                 url=None):
        if obs_id is None:
            if file_name is not None:
                obs_id = VlassName.get_obs_id_from_file_name(file_name)
            elif fname_on_disk is not None:
                obs_id = VlassName.get_obs_id_from_file_name(fname_on_disk)
            elif url is not None:
                obs_id = VlassName.get_obs_id_from_file_name(
                    url.split('/')[-1])
        super(VlassName, self).__init__(
            obs_id, COLLECTION, COLLECTION_PATTERN)
        product_id = None
        if file_name is not None:
            product_id = VlassName.get_product_id_from_file_name(file_name)
        elif fname_on_disk is not None:
            product_id = VlassName.get_product_id_from_file_name(fname_on_disk)
        elif url is not None:
            product_id = VlassName.get_product_id_from_file_name(
                url.split('/')[-1])
        self._product_id = product_id
        self.file_name = file_name
        if file_name is None:
            self.file_id = None
        else:
            self.file_id = VlassName.remove_extensions(file_name)
            self.fname_on_disk = file_name
        self.obs_id = obs_id
        if fname_on_disk is not None:
            self.file_id = VlassName.remove_extensions(fname_on_disk)
            self.fname_on_disk = fname_on_disk
            self.file_name = self.fname_on_disk.replace('.header', '')
        if url is not None:
            self.file_name = url.strip('/').split('/')[-1]
            self.fname_on_disk = self.file_name
            self.file_id = VlassName.remove_extensions(self.file_name)
            self.obs_id = VlassName.get_obs_id_from_file_name(self.file_name)
            self._url = url

    @property
    def epoch(self):
        bits = self._file_name.split('.')
        return f'{bits[0]}.{bits[1]}'

    @property
    def file_uri(self):
        """No .gz extension, unlike the default implementation."""
        return 'ad:{}/{}'.format(self.collection, self.file_name)

    @property
    def file_name(self):
        return self._file_name

    @file_name.setter
    def file_name(self, value):
        self._file_name = value

    @property
    def image_pointing_url(self):
        bits = self._file_name.split('.')
        return f'{self.tile_url}{self.epoch}.ql.{self.tile}.{bits[4]}.' \
               f'{bits[5]}.{bits[6]}.{bits[7]}/'

    @property
    def product_id(self):
        return self._product_id

    @property
    def rejected_url(self):
        return f'{scrape.QL_URL}{self.epoch}/QA_REJECTED/'

    @property
    def tile(self):
        return self._file_name.split('.')[3]

    @property
    def tile_url(self):
        return f'{scrape.QL_URL}{self.epoch}/{self.tile}/'

    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, value):
        self._url = value

    def _get_file_id(self):
        return self.file_id

    def is_valid(self):
        return True

    @staticmethod
    def get_obs_id_from_file_name(file_name):
        """The obs id is made of the VLASS epoch, tile name, and image centre
        from the file name.
        """
        bits = file_name.split('.')
        obs_id = '{}.{}.{}.{}'.format(bits[0], bits[1], bits[3], bits[4])
        return obs_id

    @staticmethod
    def get_product_id_from_file_name(file_name):
        """The product id is made of the obs id plus the string 'quicklook'.
        """
        obs_id = VlassName.get_obs_id_from_file_name(file_name)
        return '{}.quicklook'.format(obs_id)

    @staticmethod
    def get_version(uri):
        # file name looks like:
        # 'VLASS1.2.ql.T20t12.J092604+383000.10.2048.v2.I.iter1.image.
        #                'pbcor.tt0.rms.subim.fits'
        bits = mc.CaomName(uri).file_name.split('.')
        version_str = bits[7].replace('v', '')
        return mc.to_int(version_str)

    @staticmethod
    def remove_extensions(file_name):
        return file_name.replace('.fits', '').replace('.header', '')


def accumulate_wcs(bp):
    """Configure the VLASS-specific ObsBlueprint for the CAOM model
    SpatialWCS."""
    logging.debug('Begin accumulate_wcs.')
    bp.configure_position_axes((1, 2))
    bp.configure_energy_axis(3)
    bp.configure_polarization_axis(4)

    # observation level
    bp.set('Observation.type', 'OBJECT')

    # over-ride use of value from default keyword 'DATE'
    bp.clear('Observation.metaRelease')
    bp.add_fits_attribute('Observation.metaRelease', 'DATE-OBS')

    bp.clear('Observation.target.name')
    bp.add_fits_attribute('Observation.target.name', 'FILNAM04')
    bp.set('Observation.target.type', 'field')

    # Clare Chandler via JJK - 21-08-18
    bp.set('Observation.instrument.name', 'S-WIDAR')
    # From JJK - 27-08-18 - slack
    bp.set('Observation.proposal.title', 'VLA Sky Survey')
    bp.set('Observation.proposal.project', 'VLASS')
    bp.set('Observation.proposal.id', 'get_proposal_id(uri)')

    # plane level
    bp.set('Plane.calibrationLevel', '2')
    bp.clear('Plane.dataProductType')
    bp.add_fits_attribute('Plane.dataProductType', 'TYPE')

    # Clare Chandler via slack - 28-08-18
    bp.clear('Plane.provenance.name')
    bp.add_fits_attribute('Plane.provenance.name', 'ORIGIN')
    bp.set('Plane.provenance.producer', 'NRAO')
    # From JJK - 27-08-18 - slack
    bp.set('Plane.provenance.project', 'VLASS')
    bp.clear('Plane.provenance.runID')
    bp.add_fits_attribute('Plane.provenance.runID', 'FILNAM08')
    bp.clear('Plane.provenance.lastExecuted')
    bp.add_fits_attribute('Plane.provenance.lastExecuted', 'DATE')

    # VLASS data is public, says Eric Rosolowsky via JJK May 30/18
    bp.clear('Plane.metaRelease')
    bp.add_fits_attribute('Plane.metaRelease', 'DATE-OBS')
    bp.clear('Plane.dataRelease')
    bp.add_fits_attribute('Plane.dataRelease', 'DATE-OBS')

    # artifact level
    bp.clear('Artifact.productType')
    bp.set('Artifact.productType', 'get_product_type(uri)')

    # chunk level
    bp.clear('Chunk.position.axis.function.cd11')
    bp.clear('Chunk.position.axis.function.cd22')
    bp.add_fits_attribute('Chunk.position.axis.function.cd11', 'CDELT1')
    bp.set('Chunk.position.axis.function.cd12', 0.0)
    bp.set('Chunk.position.axis.function.cd21', 0.0)
    bp.add_fits_attribute('Chunk.position.axis.function.cd22', 'CDELT2')

    # Clare Chandler via JJK - 21-08-18
    bp.set('Chunk.energy.bandpassName', 'S-band')


def get_position_resolution(header):
    bmaj = header[0]['BMAJ']
    bmin = header[0]['BMIN']
    # From
    # https://open-confluence.nrao.edu/pages/viewpage.action?pageId=13697486
    # Clare Chandler via JJK - 21-08-18
    return 3600.0 * sqrt(bmaj*bmin)


def get_product_type(uri):
    if '.rms.' in uri:
        return ProductType.NOISE
    else:
        return ProductType.SCIENCE


def get_proposal_id(uri):
    caom_name = mc.CaomName(uri)
    bits = caom_name.file_name.split('.')
    return '{}.{}'.format(bits[0], bits[1])


def get_time_refcoord_value(header):
    dateobs = header[0].get('DATE-OBS')
    if dateobs is not None:
        result = ac.get_datetime(dateobs)
        if result is not None:
            return result.mjd
        else:
            return None


def update(observation, **kwargs):
    """Called to fill multiple CAOM model elements and/or attributes, must
    have this signature for import_module loading and execution.

    :param observation A CAOM Observation model instance.
    :param **kwargs Everything else."""
    logging.debug('Begin update.')

    try:

        plane_ids_to_delete = []
        mc.check_param(observation, Observation)
        for plane in observation.planes.values():
            for artifact in plane.artifacts.values():
                for part in artifact.parts.values():
                    for chunk in part.chunks:
                        if 'headers' in kwargs:
                            headers = kwargs['headers']
                            chunk.position.resolution = \
                                get_position_resolution(headers)
                            if chunk.energy is not None:
                                # A value of None per Chris, 2018-07-26
                                # Set the value to None here, because the
                                # blueprint is implemented to not set WCS
                                # information to None
                                chunk.energy.restfrq = None
            if not plane.product_id.endswith('quicklook'):
                plane_ids_to_delete.append(plane.product_id)

        for product_id in plane_ids_to_delete:
            # change handling of product ids - remove the version number
            logging.warning('Removing plane {} from {}'.format(
                product_id, observation.observation_id))
            observation.planes.pop(product_id)
        logging.debug('Done update.')
        return observation
    except mc.CadcException as e:
        tb = traceback.format_exc()
        logging.debug(tb)
        logging.error(e)
        logging.error(
            'Terminating ingestion for {}'.format(observation.observation_id))
        return None


class VlassCardinality(object):

    def __init__(self):
        self.collection = COLLECTION

    @staticmethod
    def build_blueprints(args):
        """This application relies on the caom2utils fits2caom2 ObsBlueprint
        definition for mapping FITS file values to CAOM model element
        attributes. This method builds the VLASS blueprint for a single
        artifact.

        The blueprint handles the mapping of values with cardinality of 1:1
        between the blueprint entries and the model attributes.

        :param args """
        module = importlib.import_module(__name__)
        blueprints = {}
        for ii in args.lineage:
            blueprint = ObsBlueprint(module=module)
            accumulate_wcs(blueprint)
            product_id, artifact_uri = mc.decompose_lineage(ii)
            blueprints[artifact_uri] = blueprint
        return blueprints

    def build_cardinality(self):
        pass  # TODO


def to_caom2():
    args = get_gen_proc_arg_parser().parse_args()
    vlass = VlassCardinality()
    blueprints = vlass.build_blueprints(args)
    return gen_proc(args, blueprints)


def vlass_main():
    try:
        result = to_caom2()
        logging.debug('Done {} processing.'.format(APPLICATION))
        sys.exit(result)
    except Exception as e:
        logging.error('Failed {} execution.'.format(APPLICATION))
        logging.error(e)
        tb = traceback.format_exc()
        logging.error(tb)
        sys.exit(-1)
