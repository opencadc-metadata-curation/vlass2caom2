# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2020.                            (c) 2020.
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
from caom2pipe import manage_composable as mc
from vlass2caom2 import VlassName


def visit(observation, **kwargs):
    """
    NRAO reprocesses tile + image phase center files. This visitor ensures
    the respective artifacts are removed from the observations if old versions
    of those files are removed.
    """
    mc.check_param(observation, Observation)
    url = kwargs.get('url')
    if url is None:
        raise mc.CadcException(f'Require url for cleanup augmentation of '
                               f'{observation.observation_id}')

    count = 0
    for plane in observation.planes.values():
        temp = []
        # SG - 25-03-20 - later versions of files are replacements, so just
        # automatically remove the 'older' artifacts.
        #
        # quicklook check is to cover the future case of having cubes in
        # the collection
        if len(plane.artifacts) > 2 and plane.product_id.endswith('quicklook'):
            # first - get the newest version
            max_version = 1
            for artifact in plane.artifacts.values():
                version = VlassName.get_version(artifact.uri)
                logging.error(f'version is {version} uri is {artifact.uri}')
                max_version = max(max_version, version)

            # now collect the list of artifacts not at the maximum version
            for artifact in plane.artifacts.values():
                version = VlassName.get_version(artifact.uri)
                if version != max_version:
                    temp.append(artifact.uri)

        delete_list = list(set(temp))
        for entry in delete_list:
            logging.warning(
                f'Removing artifact {entry} from observation '
                f'{observation.observation_id}, plane {plane.product_id}.')
            count += 1
            observation.planes[plane.product_id].artifacts.pop(entry)

    logging.info(
        f'Completed cleanup augmentation for {observation.observation_id}. '
        f'Remove {count} artifacts from the observation.')
    return {'artifacts': count}
