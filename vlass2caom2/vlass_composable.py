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

import tempfile

from caom2pipe import execute_composable as ec
from caom2pipe import manage_composable as mc
from vlass2caom2 import VlassName, APPLICATION, COLLECTION
from vlass2caom2 import vlass_time_bounds_augmentation

# TODO waiting on word from JJK on time implementation for VLASS
# visitors = [vlass_time_bounds_augmentation]
visitors = []


def _write_cert_to_temp_file(fqn, cert_content):
    with open(fqn, 'w') as f:
        f.write(cert_content)


def map_todo_to_obs_id(file_name):
    return VlassName.get_obs_id_from_file_name(file_name), file_name


def vlass_run():
    proxy = '/root/.ssl/cadcproxy.pem'
    ec.run_by_file(VlassName, APPLICATION, COLLECTION, map_todo_to_obs_id,
                   use_client=True, proxy=proxy, visitors=visitors)


def vlass_run_single():
    import sys
    config = mc.Config()
    config.collection = COLLECTION
    # config.working_directory = '/root/airflow'
    config.working_directory = '/usr/src/app'
    config.use_local_files = False
    config.logging_level = 'INFO'
    config.log_to_file = False
    config.task_types = [mc.TaskType.AUGMENT]
    config.resource_id = 'ivo://cadc.nrc.ca/sc2repo'
    temp = tempfile.NamedTemporaryFile()
    _write_cert_to_temp_file(temp.name, sys.argv[2])
    config.proxy = temp.name
    # config.proxy = sys.argv[2]
    config.stream = 'raw'
    file_name = sys.argv[1]
    obs_id = VlassName.get_obs_id_from_file_name(file_name)
    ec.run_single(config, VlassName, 'vlass2caom2', obs_id, file_name,
                  meta_visitors=visitors)
