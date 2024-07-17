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

import logging
from math import sqrt

from caom2 import CalibrationLevel, DataProductType, ObservationURI, PlaneURI, ProductType, TypedSet
from caom2pipe import astro_composable as ac
from caom2pipe import caom_composable as cc
from caom2pipe import manage_composable as mc

from vlass2caom2.storage_name import VlassName


__all__ = ['mapping_factory']


class BlueprintMapping(cc.TelescopeMapping):
    def __init__(self, storage_name, headers, clients, observable, observation, config):
        super().__init__(storage_name, headers, clients, observable, observation, config)

    def accumulate_blueprint(self, bp):
        """Configure the VLASS-specific ObsBlueprint for the CAOM model
        SpatialWCS."""
        self._logger.debug('Begin accumulate_blueprint.')
        super().accumulate_blueprint(bp)

        # observation level
        bp.set('Observation.type', 'OBJECT')
        bp.set('Observation.target.type', 'field')
        # Claire Chandler via JJK - 21-08-18
        bp.set('Observation.instrument.name', 'S-WIDAR')
        # From JJK - 27-08-18 - slack
        bp.set('Observation.proposal.title', 'VLA Sky Survey')
        bp.set('Observation.proposal.project', 'VLASS')
        bp.set('Observation.proposal.id', 'get_proposal_id()')

        bp.set('Plane.calibrationLevel', CalibrationLevel.PRODUCT)
        bp.set('Plane.dataProductType', DataProductType.IMAGE)

        bp.set('Plane.provenance.producer', 'NRAO')
        # From JJK - 27-08-18 - slack
        bp.set('Plane.provenance.project', 'VLASS')

        # artifact level
        bp.set('Artifact.productType', ProductType.AUXILIARY)

    def get_proposal_id(self, ext):
        caom_name = mc.CaomName(self._storage_name.file_uri)
        bits = caom_name.file_name.split('.')
        return f'{bits[0]}.{bits[1]}'

    def _update_artifact(self, artifact):
        delete_these_parts = []
        for part in artifact.parts.values():
            delete_these_chunks = []
            for index, chunk in enumerate(part.chunks):
                if chunk.position is not None:
                    chunk.position.resolution = self.get_position_resolution(0)
                if chunk.energy is not None:
                    # A value of None per Chris, 2018-07-26
                    # Set the value to None here, because the
                    # blueprint is implemented to not set WCS
                    # information to None
                    chunk.energy.restfrq = None

                if (
                    chunk.naxis == 2
                    and chunk.position is None
                    and chunk.energy is None
                    and chunk.polarization is None
                ):
                    # rms has a second HDU with BINTABLE extensions, and no WCS description (?)
                    delete_these_chunks.append(index)

            for entry in delete_these_chunks:
                self._logger.debug(f'Removing chunk index {entry} because it has no WCS.')
                part.chunks.pop(entry)

            if len(part.chunks) == 0:
                delete_these_parts.append(part.name)

        for entry in delete_these_parts:
            self._logger.debug(f'Removing part {entry} because it has no chunks.')
            artifact.parts.pop(entry)


class QuicklookMapping(BlueprintMapping):
    def __init__(self, storage_name, headers, clients, observable, observation, config):
        super().__init__(storage_name, headers, clients, observable, observation, config)

    def accumulate_blueprint(self, bp):
        """Configure the Quicklook ObsBlueprint for the CAOM model SpatialWCS."""
        self._logger.debug('Begin accumulate_wcs.')
        super().accumulate_blueprint(bp)
        bp.configure_position_axes((1, 2))
        bp.configure_energy_axis(3)
        bp.configure_polarization_axis(4)

        # observation level
        # over-ride use of value from default keyword 'DATE'
        bp.clear('Observation.metaRelease')
        bp.add_attribute('Observation.metaRelease', 'DATE-OBS')

        bp.clear('Observation.target.name')
        bp.add_attribute('Observation.target.name', 'FILNAM05')

        # plane level
        bp.set('Plane.calibrationLevel', '2')
        bp.clear('Plane.dataProductType')
        bp.add_attribute('Plane.dataProductType', 'TYPE')
        # Claire Chandler via slack - 28-08-18
        bp.clear('Plane.provenance.name')
        bp.add_attribute('Plane.provenance.name', 'ORIGIN')
        bp.clear('Plane.provenance.runID')
        bp.add_attribute('Plane.provenance.runID', 'FILNAM08')
        bp.clear('Plane.provenance.lastExecuted')
        bp.add_attribute('Plane.provenance.lastExecuted', 'DATE')
        # VLASS data is public, says Eric Rosolowsky via JJK May 30/18
        bp.clear('Plane.metaRelease')
        bp.add_attribute('Plane.metaRelease', 'DATE-OBS')
        bp.clear('Plane.dataRelease')
        bp.add_attribute('Plane.dataRelease', 'DATE-OBS')

        # artifact level
        bp.clear('Artifact.productType')
        bp.set('Artifact.productType', 'get_product_type()')
        bp.set('Artifact.releaseType', 'data')

        # chunk level
        bp.clear('Chunk.position.axis.function.cd11')
        bp.clear('Chunk.position.axis.function.cd22')
        bp.add_attribute('Chunk.position.axis.function.cd11', 'CDELT1')
        bp.set('Chunk.position.axis.function.cd12', 0.0)
        bp.set('Chunk.position.axis.function.cd21', 0.0)
        bp.add_attribute('Chunk.position.axis.function.cd22', 'CDELT2')

        # Claire Chandler via JJK - 21-08-18
        bp.set('Chunk.energy.bandpassName', 'S-band')

    def get_position_resolution(self, ext):
        bmaj = self._headers[ext].get('BMAJ')
        bmin = self._headers[ext].get('BMIN')
        # From
        # https://open-confluence.nrao.edu/pages/viewpage.action?pageId=13697486
        # Claire Chandler via JJK - 21-08-18
        result = None
        if bmaj is not None and bmaj != 'INF' and bmin is not None and bmin != 'INF':
            result = 3600.0 * sqrt(bmaj * bmin)
        return result

    def get_product_type(self, ext):
        result = ProductType.AUXILIARY
        if self._storage_name.product_id.endswith('continuum_imaging'):
            if '.tt0.' in self._storage_name.file_uri and '.rms.' not in self._storage_name.file_uri:
                result = ProductType.SCIENCE
        else:
            if '.rms.' in self._storage_name.file_uri:
                result = ProductType.NOISE
            elif self._storage_name.file_uri.endswith('.csv'):
                result = ProductType.AUXILIARY
            else:
                result = ProductType.SCIENCE
        return result

    def get_time_refcoord_value(self, ext):
        dateobs = self._headers[ext].get('DATE-OBS')
        if dateobs is not None:
            result = ac.get_datetime(dateobs)
            if result is not None:
                return result.mjd
            else:
                return None


class ContinuumMapping(QuicklookMapping):
    def __init__(self, storage_name, headers, clients, observable, observation, config):
        super().__init__(storage_name, headers, clients, observable, observation, config)

    def accumulate_blueprint(self, bp, application=None):
        super().accumulate_blueprint(bp)
        bp.clear('Observation.target.name')
        bp.add_attribute('Observation.target.name', 'FILNAM06')
        bp.clear('Observation.telescope.geoLocationX')
        bp.add_attribute('Observation.telescope.geoLocationX', 'OBSGEO-X')
        bp.clear('Observation.telescope.geoLocationY')
        bp.add_attribute('Observation.telescope.geoLocationY', 'OBSGEO-Y')
        bp.clear('Observation.telescope.geoLocationZ')
        bp.add_attribute('Observation.telescope.geoLocationZ', 'OBSGEO-Z')

        bp.set('Plane.calibrationLevel', CalibrationLevel.PRODUCT)
        bp.set('Plane.dataProductType', DataProductType.IMAGE)
        bp.clear('Plane.provenance.runID')
        bp.add_attribute('Plane.provenance.runID', 'FILNAM09')

        bp.add_attribute('Chunk.energy.restfrq', 'RESTFREQ')


class ChannelCubeMapping(ContinuumMapping):
    def __init__(self, storage_name, headers, clients, observable, observation, config):
        super().__init__(storage_name, headers, clients, observable, observation, config)

    def accumulate_blueprint(self, bp, application=None):
        super().accumulate_blueprint(bp)
        bp.clear('Observation.target.name')
        bp.add_attribute('Observation.target.name', 'FILNAM06')


class Catalog(BlueprintMapping):
    def __init__(self, storage_name, headers, clients, observable, observation, config):
        super().__init__(storage_name, headers, clients, observable, observation, config)
        self._config = config
        self._provenance_storage_name = None

    def accumulate_blueprint(self, bp):
        """
        # Executing run_getalpha  2021-06-30 Py3 Verison 1.7  on 2023-11-01T08:51:24.739175
        # ImageFile=./VLASS2.1.se.T13t10.J063820+113000.06.2048.v1.I.iter3
        # ImageReferenceDirection="ICRS 06:38:20.21611 +11.29.59.8957"
        # RestoringBeam="2.48arcsec 2.12arcsec -8.69deg"
        #
        # Gaussian list for VLASS2.1.se.T13t10.J063820+113000.06.2048.v1.I.iter3.image.pbcor.tt0.subim.fits
        # Generated by PyBDSM version 1.10.1
        # Reference frequency of the detection ("ch0") image: 3.00000e+09 Hz
        # Equinox : 2000.0
        """
        super().accumulate_blueprint(bp)
        # plane level
        bp.set('Plane.calibrationLevel', '4')
        bp.set('Plane.dataProductType', 'http://www.opencadc.org/caom2/DataProductType#catalog')
        bp.set('Plane.provenance.name', 'get_provenance_name()')
        bp.set('Plane.provenance.project', 'VLASS')
        bp.set('Plane.provenance.producer', 'NRAO')
        bp.set('Plane.provenance.runID', self._storage_name.run_id)
        bp.set('Plane.provenance.lastExecuted', 'get_provenance_last_executed()')
        bp.set('Plane.provenance.version', 'get_provenance_version()')
        bp.set('Plane.provenance.inputs', 'get_provenance_inputs()')
        # VLASS data is public
        bp.set('Plane.dataRelease', 'get_provenance_last_executed()')
        bp.set('Plane.metaRelease', 'get_provenance_last_executed()')
        bp.set('Artifact.productType', ProductType.SCIENCE)

    def get_provenance_inputs(self, ext):
        # 'Gaussian list for VLASS2.1.se.T13t10.J063820+113000.06.2048.v1.I.iter3.image.pbcor.tt0.subim.fits'
        plane_inputs = TypedSet(PlaneURI)
        fqn = mc.search_for_file(self._storage_name, self._config.working_directory)
        with open(fqn) as f:
            for line in f:
                if 'Gaussian list for' in line:
                    temp = line.split()[-1]
                    if temp is not None:
                        self._provenance_storage_name = VlassName(temp)
                        obs_member_uri_str = mc.CaomName.make_obs_uri_from_obs_id(
                            self._storage_name.collection, self._provenance_storage_name.obs_id
                        )
                        obs_member_uri = ObservationURI(obs_member_uri_str)
                        plane_uri = PlaneURI.get_plane_uri(obs_member_uri, self._provenance_storage_name.product_id)
                        plane_inputs.add(plane_uri)
                        self._logger.debug(f'Adding PlaneURI {plane_uri}')
                    break
        return plane_inputs

    def get_provenance_last_executed(self, ext):
        # Executing run_getalpha  2021-06-30 Py3 Verison 1.7  on 2023-11-01T08:51:24.739175
        fqn = mc.search_for_file(self._storage_name, self._config.working_directory)
        result = None
        with open(fqn) as f:
            for line in f:
                if 'Executing' in line:
                    result = line.split()[-1]
                    break
        return result

    def get_provenance_name(self, ext):
        # Executing run_getalpha  2021-06-30 Py3 Verison 1.7  on 2023-11-01T08:51:24.739175
        fqn = mc.search_for_file(self._storage_name, self._config.working_directory)
        result = None
        with open(fqn) as f:
            for line in f:
                if 'Executing' in line:
                    result = line.split()[2]
                    break
        return result

    def get_provenance_version(self, ext):
        # Executing run_getalpha  2021-06-30 Py3 Verison 1.7  on 2023-11-01T08:51:24.739175
        fqn = mc.search_for_file(self._storage_name, self._config.working_directory)
        result = None
        with open(fqn) as f:
            for line in f:
                if 'Executing' in line:
                    bits = line.split()
                    result = f'{bits[3]} {bits[4]} {bits[5]} {bits[6]}'
                    break
        return result

    def _update_artifact(self, artifact):
        for plane in self._observation.planes.values():
            if plane.product_id == self._provenance_storage_name.product_id:
                for provenance_artifact in plane.artifacts.values():
                    if provenance_artifact.uri == self._provenance_storage_name.file_uri:
                        for part in provenance_artifact.parts.values():
                            copied_part = cc.copy_part(part)
                            artifact.parts.add(copied_part)
                            for chunk in part.chunks:
                                copied_chunk = cc.copy_chunk(chunk)
                                copied_part.chunks.append(copied_chunk)
                                # no cutouts to be done from the CSV file, so null out those bits
                                copied_chunk.naxis = None
                                copied_chunk.polarization_axis = None
                                copied_chunk.energy_axis = None
                                copied_chunk.position_axis_1 = None
                                copied_chunk.position_axis_2 = None
                                copied_chunk.time_axis = None


def mapping_factory(storage_name, headers, clients, observable, observation, config):
    if storage_name.is_single_epoch:
        if storage_name.is_catalog:
            result = Catalog(storage_name, headers, clients, observable, observation, config)
        else:
            result = ContinuumMapping(storage_name, headers, clients, observable, observation, config)
    elif storage_name.is_quicklook:
        result = QuicklookMapping(storage_name, headers, clients, observable, observation, config)
    elif storage_name.is_channel_cube:
        result = ChannelCubeMapping(storage_name, headers, clients, observable, observation, config)
    elif storage_name.is_catalog:
        result = BlueprintMapping(storage_name, headers, clients, observable, observation, config)
    else:
        raise mc.CadcException(f'Do not understand {storage_name} for mapping construction.')
    logging.debug(f'Using {result.__class__.__name__} for mapping.')
    return result

