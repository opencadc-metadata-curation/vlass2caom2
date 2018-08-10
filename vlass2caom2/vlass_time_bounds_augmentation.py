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

from caom2 import Observation
from caom2pipe import astro_composable as ac
from caom2pipe import manage_composable as mc
from vlass2caom2 import VlassName


def visit(observation, **kwargs):
    mc.check_param(observation, Observation)

    working_dir = './'
    if 'working_directory' in kwargs:
        working_dir = kwargs['working_directory']

    count = 0
    for i in observation.planes:
        plane = observation.planes[i]
        logging.debug('working on plane {}'.format(plane.product_id))
        _augment(working_dir, observation.observation_id, plane)
        count += 1
    return {'planes': count}


def _augment(working_dir, obs_id, plane):
    # conversation with JJK, 2018-08-08 - until such time as VLASS becomes
    # a dynamic collection, rely on the time information as provided for all
    # observations as retrieved on this date from:
    #
    # https://archive.nrao.edu/archive/ArchiveQuery?PROTOCOL=TEXT-stream
    # &QUERYTYPE=ARCHIVE&PROJECT_CODE=VLASS1.1 > observing_log.csv
    #
    # This contains a list of the Measurement Sets (ms files) that were
    # acquired as part of the VLASS processing.

    logging.debug('get content of all the VLASS observations from 2018-08-08')
    import os
    csv_file = mc.read_csv_file(
        os.path.join(working_dir, 'ArchiveQuery-2018-08-08.csv'))

    # To determine which measurement set went into which QL data product
    # need to look at the casa_commands.log file for the QL data product.
    #
    # the casa_commands.log file is the same for the science and noise files

    logging.debug('retrieve casa_commands.log file with measurement set info')
    log_url = VlassName.make_url_from_obs_id(obs_id)
    log_file_content = mc.read_url_file(log_url)

    logging.debug('retrieve the measurement set information from the logs')
    measurement_sets = _find_measurement_sets(log_file_content)

    logging.debug('build time bounds information from measurement set info')
    _augment_plane(plane, measurement_sets, csv_file)


def _augment_plane(plane, measurement_sets, csv_file):
    bounds = None
    for ii in measurement_sets:
        # Stripping the .ms. off the end allows this record to be found in
        # the observation log CSV content.
        mset = ii.replace('.ms', '').replace('\'', '')
        for jj in csv_file:
            if mset in jj:
                start_date = ac.get_datetime(jj[5].strip())
                end_date = ac.get_datetime(jj[6].strip())
                exposure = end_date - start_date
                bounds = ac.build_plane_time(start_date, end_date, exposure)
                break
    plane.time = bounds


def _find_measurement_sets(from_content):
    # find the line that contains ‘hifv_importdata’ and taking the value of
    # the ‘vis’ parameter from that command, eg.
    #
    # vis=['VLASS1.1.sb34916486.eb35006898.58156.86241219907.ms']
    #
    # provides an  array of the input measurement sets that can be looked up
    # in the CSV file stored with this project

    result = None
    for ii in from_content:
        if 'hifv_importdata' in ii:
            result = ii.split('vis=[')[1].split(']')[0].split(',')
            break
    return result
