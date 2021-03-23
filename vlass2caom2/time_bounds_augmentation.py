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

from vlass2caom2 import scrape

obs_metadata = None


def visit(observation, **kwargs):
    mc.check_param(observation, Observation)
    cadc_client = kwargs.get('cadc_client')
    count = 0
    if cadc_client is None:
        logging.warning('No cadc_client parameter, no connection for input '
                        'metadata. Stopping time_bounds_augmentation.')

    else:
        # conversation with JJK, 2018-08-08 - until such time as VLASS becomes
        # a dynamic collection, rely on the time information as provided for all
        # observations as retrieved on this date from:
        #
        # https://archive-new.nrao.edu/vlass/weblog/quicklook/*

        count = 0
        for plane in observation.planes.values():
            for artifact in plane.artifacts.values():
                if len(artifact.parts) > 0:
                    logging.debug(f'working on artifact {artifact.uri}')
                    version, reference = _augment_artifact(
                        observation.observation_id, artifact)
                    if version is not None:
                        plane.provenance.version = version
                    if reference is not None:
                        plane.provenance.reference = reference
                        count += 1
        logging.info(f'Completed time bounds augmentation for '
                     f'{observation.observation_id}')
        global obs_metadata
        obs_metadata = None
    return {'artifacts': count}


def _augment_artifact(obs_id, artifact):
    chunk = artifact.parts['0'].chunks[0]
    bounds = None
    exposure = None
    version = None
    reference = None
    found = False
    logging.debug(f'Scrape for time metadata for {obs_id}')
    global obs_metadata
    if obs_metadata is None:
        obs_metadata = scrape.retrieve_obs_metadata(obs_id)
    if obs_metadata is not None and len(obs_metadata) > 0:
        bounds, exposure = _build_time(
            obs_metadata.get('Observation Start'),
            obs_metadata.get('Observation End'),
            obs_metadata.get('On Source'))
        version = obs_metadata.get('Pipeline Version')
        reference = obs_metadata.get('reference')
        found = True
    else:
        logging.warning(f'Found no time metadata for {obs_id}')

    if found:
        time_axis = CoordAxis1D(Axis('TIME', 'd'))
        time_axis.bounds = bounds
        chunk.time = TemporalWCS(time_axis)
        chunk.time.exposure = exposure
        chunk.time_axis = None
        return version, reference
    else:
        return None, None


def _build_time(start, end, tos):
    bounds = CoordBounds1D()
    if start is not None and end is not None:
        start_date = ac.get_datetime(start)
        start_date.format = 'mjd'
        end_date = ac.get_datetime(end)
        end_date.format = 'mjd'
        start_ref_coord = RefCoord(0.5, start_date.value)
        end_ref_coord = RefCoord(1.5, end_date.value)
        bounds.samples.append(CoordRange1D(start_ref_coord,
                                           end_ref_coord))
    exposure = None
    if tos is not None:
        exposure = float(ac.get_timedelta_in_s(tos))
    return bounds, exposure
