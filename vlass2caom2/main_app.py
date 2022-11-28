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

import traceback

from math import sqrt

from caom2 import CalibrationLevel, DataProductType, ProductType
from caom2utils import update_artifact_meta
from caom2pipe import astro_composable as ac
from caom2pipe import caom_composable as cc
from caom2pipe import manage_composable as mc


__all__ = ['APPLICATION', 'mapping_factory']

APPLICATION = 'vlass2caom2'


class BlueprintMapping(cc.TelescopeMapping):
    def __init__(self, storage_name, headers, clients):
        super().__init__(storage_name, headers, clients)

    def accumulate_blueprint(self, bp, application=None):
        """Configure the VLASS-specific ObsBlueprint for the CAOM model
        SpatialWCS."""
        self._logger.debug('Begin accumulate_wcs.')
        super().accumulate_blueprint(bp, APPLICATION)

        # observation level
        bp.set('Observation.type', 'OBJECT')
        bp.set('Observation.target.type', 'field')
        # Clare Chandler via JJK - 21-08-18
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


class QuicklookMapping(BlueprintMapping):
    def __init__(self, storage_name, headers, clients):
        super().__init__(storage_name, headers, clients)

    def accumulate_blueprint(self, bp, application=None):
        """Configure the Quicklook ObsBlueprint for the CAOM model SpatialWCS."""
        self._logger.debug('Begin accumulate_wcs.')
        super().accumulate_blueprint(bp, APPLICATION)
        bp.configure_position_axes((1, 2))
        bp.configure_energy_axis(3)
        bp.configure_polarization_axis(4)

        # observation level
        # over-ride use of value from default keyword 'DATE'
        bp.clear('Observation.metaRelease')
        bp.add_attribute('Observation.metaRelease', 'DATE-OBS')

        bp.clear('Observation.target.name')
        bp.add_attribute('Observation.target.name', 'FILNAM04')

        # plane level
        bp.set('Plane.calibrationLevel', '2')
        bp.clear('Plane.dataProductType')
        bp.add_attribute('Plane.dataProductType', 'TYPE')
        # Clare Chandler via slack - 28-08-18
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

        # Clare Chandler via JJK - 21-08-18
        bp.set('Chunk.energy.bandpassName', 'S-band')

    def get_position_resolution(self, ext):
        bmaj = self._headers[ext]['BMAJ']
        bmin = self._headers[ext]['BMIN']
        # From
        # https://open-confluence.nrao.edu/pages/viewpage.action?pageId=13697486
        # Clare Chandler via JJK - 21-08-18
        result = None
        if bmaj is not None and bmaj != 'INF' and bmin is not None and bmin != 'INF':
            result = 3600.0 * sqrt(bmaj * bmin)
        return result

    def get_product_type(self, ext):
        if '.rms.' in self._storage_name.file_uri:
            return ProductType.NOISE
        elif self._storage_name.file_uri.endswith('.csv'):
            return ProductType.AUXILIARY
        else:
            return ProductType.SCIENCE

    def get_time_refcoord_value(self, ext):
        dateobs = self._headers[ext].get('DATE-OBS')
        if dateobs is not None:
            result = ac.get_datetime(dateobs)
            if result is not None:
                return result.mjd
            else:
                return None

    def update(self, observation, file_info):
        """Called to fill multiple CAOM model elements and/or attributes, must
        have this signature for import_module loading and execution.
        """
        self._logger.debug('Begin update.')
        try:
            for plane in observation.planes.values():
                for artifact in plane.artifacts.values():
                    if artifact.uri != self._storage_name.file_uri:
                        continue
                    update_artifact_meta(artifact, file_info)
                    for part in artifact.parts.values():
                        for chunk in part.chunks:
                            if chunk.position is not None:
                                chunk.position.resolution = (
                                    self.get_position_resolution(0)
                                )
                            if chunk.energy is not None:
                                # A value of None per Chris, 2018-07-26
                                # Set the value to None here, because the
                                # blueprint is implemented to not set WCS
                                # information to None
                                chunk.energy.restfrq = None

            self._logger.debug('Done update.')
            return observation
        except mc.CadcException as e:
            tb = traceback.format_exc()
            self._logger.debug(tb)
            self._logger.error(e)
            self._logger.error(
                f'Terminating ingestion for {observation.observation_id}'
            )
            return None


class ContinuumMapping(QuicklookMapping):
    def __init__(self, storage_name, headers, clients):
        super().__init__(storage_name, headers, clients)

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


def mapping_factory(storage_name, headers, clients):
    if storage_name.is_catalog():
        return BlueprintMapping(storage_name, headers, clients)
    elif storage_name.is_quicklook():
        return QuicklookMapping(storage_name, headers, clients)
    else:
        return ContinuumMapping(storage_name, headers, clients)
