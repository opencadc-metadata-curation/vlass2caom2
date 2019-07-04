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
import sys
import tempfile
import traceback

from datetime import datetime

from caom2pipe import execute_composable as ec
from caom2pipe import manage_composable as mc
from vlass2caom2 import VlassName, APPLICATION
from vlass2caom2 import time_bounds_augmentation, quality_augmentation
from vlass2caom2 import position_bounds_augmentation
from vlass2caom2 import scrape, utils, work


VLASS_BOOKMARK = 'vlass_timestamp'

meta_visitors = [time_bounds_augmentation, quality_augmentation,
                 position_bounds_augmentation]
data_visitors = []


def _run_by_file():
    """uses a todo file with URLs, which is the only way to find
    context information about QA_REJECTED.
    """
    config = mc.Config()
    config.get_executors()
    config.features.use_urls = True
    with open(config.work_fqn) as f:
        todo_list_length = sum(1 for _ in f)
    if todo_list_length > 0:
        work.init_web_log()
        result = ec.run_by_file_prime(config, VlassName, APPLICATION,
                                      meta_visitors,
                                      data_visitors=data_visitors,
                                      chooser=None)
    else:
        logging.info('No records to process.')
        result = 0
    return result


def run_by_file():
    """Wraps _run_by_file in exception handling."""
    try:
        result = _run_by_file()
        sys.exit(result)
    except Exception as e:
        logging.error(e)
        tb = traceback.format_exc()
        logging.debug(tb)
        sys.exit(-1)


def _run_single():
    """expects a single file name on the command line"""
    import sys
    config = mc.Config()
    config.get_executors()
    file_name = sys.argv[1]
    if config.features.use_file_names:
        vlass_name = VlassName(file_name=file_name)
    elif config.features.use_urls:
        vlass_name = VlassName(url=file_name)
    else:
        vlass_name = VlassName(obs_id=sys.argv[1])
    if config.features.run_in_airflow:
        temp = tempfile.NamedTemporaryFile()
        mc.write_to_file(temp.name, sys.argv[2])
        config.proxy_fqn = temp.name
    else:
        config.proxy_fqn = sys.argv[2]
    return ec.run_single(config, vlass_name, APPLICATION,
                         meta_visitors=meta_visitors,
                         data_visitors=data_visitors)


def run_single():
    """Wraps _run_single in exception handling."""
    try:
        result = _run_single()
        sys.exit(result)
    except Exception as e:
        logging.error(e)
        tb = traceback.format_exc()
        logging.debug(tb)
        sys.exit(-1)


def _run_state_2():
    """Uses a state file with a timestamp to control which quicklook
    files will be retrieved from VLASS.

    Ingestion is based on URLs, because a URL that contains the phrase
    'QA_REJECTED' is the only way to tell if the attribute 'requirements'
    should be set to 'fail', or not.
    """
    config = mc.Config()
    config.get_executors()
    return ec.run_from_state(config, VlassName, APPLICATION, meta_visitors,
                             data_visitors, VLASS_BOOKMARK,
                             work.NraoPageScrape())


def _run_state():
    """Uses a state file with a timestamp to control which quicklook
    files will be retrieved from VLASS.

    Ingestion is based on URLs, because a URL that contains the phrase
    'QA_REJECTED' is the only way to tell if the attribute 'requirements'
    should be set to 'fail', or not.
    """
    config = mc.Config()
    config.get_executors()
    config.features.use_urls = True
    state = mc.State(config.state_fqn)
    start_time = utils.get_bookmark(state)
    logging.info('Starting at {}'.format(start_time))
    logger = logging.getLogger()
    logger.setLevel(config.logging_level)
    todo_list, max_date = scrape.build_file_url_list(start_time)
    if len(todo_list) == 0:
        logging.info('No items to process after {}'.format(start_time))
        return

    logging.info('{} items to process. Max date will be {}'.format(
        len(todo_list), max_date))
    work.init_web_log()
    count = 0
    current_timestamp = start_time
    organizer = ec.OrganizeExecutes(config, chooser=None)
    organizer.complete_record_count = len(todo_list)
    for timestamp, urls in todo_list.items():
        for url in urls:
            logging.info('{}: Process {}'.format(APPLICATION, url))
            vlass_name = VlassName(url=url)
            ec.run_single_from_state(organizer, config, vlass_name,
                                     APPLICATION, meta_visitors=meta_visitors,
                                     data_visitors=data_visitors)
            count += 1
            current_timestamp = timestamp
            if count % 10 == 0:
                state.save_state('vlass_timestamp',
                                 _make_time_str(current_timestamp))
                logging.info('Saving timestamp {}'.format(current_timestamp))
    state.save_state('vlass_timestamp', _make_time_str(current_timestamp))
    logging.info(
        'Done {}, saved state is {}'.format(APPLICATION, current_timestamp))


def run_state():
    """Wraps _run_state in exception handling."""
    try:
        _run_state_2()
        sys.exit(0)
    except Exception as e:
        logging.error(e)
        tb = traceback.format_exc()
        logging.debug(tb)
        sys.exit(-1)


def _make_time_str(value):
    # 01-May-2019 15:40 - support the format of what's visible on the
    # web page, to make it easy to cut-and-paste
    # go from a float, return a string
    temp = datetime.fromtimestamp(value)
    return datetime.strftime(temp, '%d-%b-%Y %H:%M')
