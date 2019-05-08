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


def visit(observation, **kwargs):
    mc.check_param(observation, Observation)

    # conversation with JJK, 2018-08-08 - until such time as VLASS becomes
    # a dynamic collection, rely on the time information as provided for all
    # observations as retrieved on this date from:
    #
    # https://archive-new.nrao.edu/vlass/weblog/quicklook/*
    #
    # The lowest-level index.html files are scraped to create a csv file
    # with observation ID, start time, end time, and exposure time.

    count = 0
    for i in observation.planes:
        plane = observation.planes[i]
        for j in plane.artifacts:
            artifact = plane.artifacts[j]
            logging.debug('working on artifact {}'.format(artifact.uri))
            version, reference = _augment(observation.observation_id, artifact)
            if version is not None and reference is not None:
                plane.provenance.version = version
                plane.provenance.reference = reference
                count += 1
    logging.info('Completed time bounds augmentation for {}'.format(
        observation.observation_id))
    return {'artifacts': count}


def _augment(obs_id, artifact):

    # note the location of this file is hard-coded in the container
    # structure, because so much about this is broken anyway
    #
    logging.debug('get content of all the VLASS observations from 2018-08-08')
    csv_file = mc.read_csv_file('/usr/src/ArchiveQuery-2018-08-15.csv')

    logging.debug('build time bounds information from measurement set info')
    version, reference = _augment_artifact(obs_id, artifact, csv_file)
    return version, reference


def _augment_artifact(obs_id, artifact, csv_file):
    chunk = artifact.parts['0'].chunks[0]
    bounds = None
    exposure = None
    version = None
    reference = None
    found = False
    for ii in csv_file:
        if obs_id in ii:
            bounds, exposure = _build_from_row(ii)
            version = ii[1].strip()
            reference = ii[2].strip()
            found = True
            break

    if not found:
        result = scrape.retrieve_obs_metadata(obs_id)
        if result is not None:
            bounds, exposure = _build_time(result['Observation Start'],
                                           result['Observation End'],
                                           result['On Source'])
            version = result['Pipeline Version']
            reference = result['reference']
            found = True

    if found:
        time_axis = CoordAxis1D(Axis('TIME', 'd'))
        time_axis.bounds = bounds
        chunk.time = TemporalWCS(time_axis)
        chunk.time.exposure = exposure
        count = 0
        for ii in [chunk.position, chunk.energy, chunk.polarization,
                   chunk.observable]:
            if ii is not None:
                if ii is chunk.position:
                    count += 2
                else:
                    count += 1
        chunk.time_axis = count + 1
        chunk.naxis = count + 1
        return version, reference
    else:
        return None, None


def _build_from_row(row):
    return _build_time(row[3].strip(), row[4].strip(), row[5].strip())


def _build_time(start, end, tos):
    bounds = CoordBounds1D()
    start_date = ac.get_datetime(start)
    end_date = ac.get_datetime(end)
    start_date.format = 'mjd'
    end_date.format = 'mjd'
    exposure = float(ac.get_timedelta_in_s(tos))
    start_ref_coord = RefCoord(0.5, start_date.value)
    end_ref_coord = RefCoord(1.5, end_date.value)
    bounds.samples.append(CoordRange1D(start_ref_coord,
                                       end_ref_coord))
    return bounds, exposure
