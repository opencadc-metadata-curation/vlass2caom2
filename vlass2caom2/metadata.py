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

import logging

from caom2pipe import manage_composable as mc
from datetime import datetime, timezone
from dateutil import tz
from vlass2caom2 import scrape
from vlass2caom2 import storage_name as sn


class VLASSCache(object):
    def __init__(self):
        # if None, refresh the cache
        self._refresh_bookmark = None
        self._qa_rejected_obs_ids = []
        self._tz = tz.gettz('US/Socorro')
        self._new_bookmark = datetime.now(tz=timezone.utc)
        self._logger = logging.getLogger(__class__.__name__)

    def _refresh(self):
        start_date = self._refresh_bookmark
        if self._refresh_bookmark is None:
            start_date = datetime(
                year=2017, month=1, day=1, hour=0, tzinfo=self._tz
            )
        session = mc.get_endpoint_session()
        todo_list, ignore_max_date = scrape.build_qa_rejected_todo(
            start_date, session
        )

        for timestamp, urls in todo_list.items():
            for url in urls:
                # there are trailing slashes on the NRAO VLASS QL page
                obs_id = sn.VlassName.get_obs_id_from_file_name(
                    url.split('/')[-2]
                )
                self._logger.debug(f'Add QA REJECTED {obs_id}.')
                self._qa_rejected_obs_ids.append(obs_id)
        self._refresh_bookmark = self._new_bookmark

    def is_qa_rejected(self, obs_id):
        # if the cache has not been updated this run refresh the cache
        if self._refresh_bookmark is None:
            self._logger.info('Refresh QA REJECTED cache.')
            self._refresh()

        # check the cache
        return obs_id in self._qa_rejected_obs_ids


cache = VLASSCache()
