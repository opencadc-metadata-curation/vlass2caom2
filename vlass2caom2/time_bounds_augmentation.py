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

import logging

from caom2 import Observation, RefCoord, CoordBounds1D, CoordRange1D
from caom2 import TemporalWCS, CoordAxis1D, Axis
from caom2pipe import astro_composable as ac
from caom2pipe import manage_composable as mc


def visit(observation, **kwargs):
    mc.check_param(observation, Observation)
    # conversation with JJK, 2018-08-08 - until such time as VLASS becomes
    # a dynamic collection, rely on the time information as provided for
    # all observations as retrieved on this date from:
    #
    # https://archive-new.nrao.edu/vlass/weblog/quicklook/*

    metadata_reader = kwargs.get('metadata_reader')
    if metadata_reader is None:
        raise mc.CadcException('Require a metadata_reader.')
    storage_name = kwargs.get('storage_name')
    if storage_name is None:
        raise mc.CadcException('Require a storage name.')
    clients = kwargs.get('clients')
    if clients is None:
        logging.warning(f'Scraping, so no access to Temporal metadata inputs.')
        return observation

    count = 0
    for plane in observation.planes.values():
        for artifact in plane.artifacts.values():
            if artifact.uri != storage_name.file_uri:
                continue
            if len(artifact.parts) > 0:
                logging.debug(f'working on artifact {artifact.uri}')
                version, reference = _augment_artifact(
                    observation.observation_id, artifact, metadata_reader, storage_name
                )
                if version is not None:
                    plane.provenance.version = version
                if reference is not None:
                    plane.provenance.reference = reference
                    count += 1
    logging.info(
        f'Completed time bounds augmentation for '
        f'{observation.observation_id}'
    )
    return observation


def _augment_artifact(obs_id, artifact, metadata_reader, storage_name):
    chunk = artifact.parts['0'].chunks[0]
    version = None
    reference = None
    logging.debug(f'Scrape for time metadata for {obs_id}')
    obs_metadata = metadata_reader.get_web_log_info(storage_name)
    if obs_metadata is None or len(obs_metadata) == 0:
        logging.warning(f'Found no time metadata for {obs_id}')
    else:
        bounds, exposure = _build_time(
            obs_metadata.get('Observation Start'), obs_metadata.get('Observation End'), obs_metadata.get('On Source')
        )
        time_axis = CoordAxis1D(Axis('TIME', 'd'))
        time_axis.bounds = bounds
        chunk.time = TemporalWCS(time_axis)
        chunk.time.exposure = exposure
        # JJK TIMESYS=UTC, which is in the headers of the VLASS2.1 files at least
        chunk.time.timesys = 'UTC'
        chunk.time_axis = None
        version = obs_metadata.get('Pipeline Version')
        reference = obs_metadata.get('reference')
    return version, reference


def _build_time(start, end, tos):
    bounds = CoordBounds1D()
    if start is not None and end is not None:
        start_date = ac.get_datetime_mjd(start)
        end_date = ac.get_datetime_mjd(end)
        start_ref_coord = RefCoord(0.5, start_date.value)
        end_ref_coord = RefCoord(1.5, end_date.value)
        bounds.samples.append(CoordRange1D(start_ref_coord, end_ref_coord))
    exposure = None
    if tos is not None:
        exposure = float(ac.get_timedelta_in_s(tos))
    return bounds, exposure
