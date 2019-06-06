# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2019.                            (c) 2019.
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

import io
import logging
import os
import traceback

from datetime import datetime
from urllib import parse as parse

from astropy.io.votable import parse_single_table

from cadcutils import net
from cadctap import CadcTapClient
from caom2pipe import manage_composable as mc

from vlass2caom2 import APPLICATION, scrape, utils

__all__ = ['validate', 'generate_reconciliation_todo']

NRAO_STATE = 'nrao_state.csv'
VALIDATE_STATE = 'validate_state.yml'
ISO_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'


def read_file_list_from_archive(config):
    ad_resource_id = 'ivo://cadc.nrc.ca/ad'
    agent = '{}/{}'.format(APPLICATION, '1.0')
    subject = net.Subject(certificate=config.proxy_fqn)
    client = net.BaseWsClient(resource_id=ad_resource_id,
                              subject=subject, agent=agent, retry=True)
    query_meta = "SELECT fileName FROM archive_files WHERE " \
                 "archiveName = '{}'".format(config.archive)
    data = {'QUERY': query_meta, 'LANG': 'ADQL', 'FORMAT': 'csv'}
    logging.debug('Query is {}'.format(query_meta))
    try:
        response = client.get('https://{}/ad/sync?{}'.format(
            client.host, parse.urlencode(data)), cert=config.proxy_fqn)
        if response.status_code == 200:
            # ignore the column name as the first part of the response
            artifact_files_list = response.text.split()[1:]
            return artifact_files_list
        else:
            raise mc.CadcException('Query failure {!r}'.format(response))
    except Exception as e:
        raise mc.CadcException('Failed ad content query: {}'.format(e))


def read_list_from_caom(config):
    query = "SELECT A.uri FROM caom2.Observation AS O " \
            "JOIN caom2.Plane AS P ON O.obsID = P.obsID " \
            "JOIN caom2.Artifact AS A ON P.planeID = A.planeID " \
            "WHERE O.collection='{}'".format(config.archive)
    subject = net.Subject(certificate=config.proxy_fqn)
    tap_client = CadcTapClient(subject, resource_id=config.tap_id)
    buffer = io.BytesIO()
    tap_client.query(query, output_file=buffer)
    temp = parse_single_table(buffer).to_table()
    return [ii.decode().replace('ad:{}/'.format(config.archive), '').strip()
            for ii in temp['uri']]


def read_list_from_nrao(nrao_state_fqn, config_state_fqn):
    if os.path.exists(nrao_state_fqn):
        logging.error('file exists')
        temp = []
        with open(nrao_state_fqn, 'r') as f:
            for line in f:
                value = line.split(',')[1].split('/')[-1]
                temp.append(value.strip())
    else:
        logging.error('file does not exist {}'.format(nrao_state_fqn))
        start_date = datetime.strptime('01Jan1990 00:00',
                                       scrape.PAGE_TIME_FORMAT)
        state = mc.State(config_state_fqn)
        end_date = utils.get_bookmark(state)
        logging.error(
            'end_date is {} from {}'.format(end_date, config_state_fqn))
        vlass_list, vlass_date = scrape.build_file_url_list(start_date)
        temp = []
        with open(nrao_state_fqn, 'w') as f:
            for timestamp, urls in vlass_list.items():
                ts_date = datetime.fromtimestamp(timestamp)
                if ts_date <= end_date:
                    for url in urls:
                        f.write('{}, {}\n'.format(
                            ts_date.isoformat(), url.strip()))
                        value = url.split('/')[-1]
                        temp.append(value.strip())
    result = list(set(temp))
    return result


def _find_missing(compare_this, to_this):
    return [ii for ii in compare_this if ii not in to_this]


def generate_reconciliation_todo():
    config = mc.Config()
    config.get_executors()
    logging.error('working dir is {}'.format(config.working_directory))
    result_fqn = os.path.join(config.working_directory, VALIDATE_STATE)
    logging.error('result fqn is {}'.format(result_fqn))
    if os.access(result_fqn, os.R_OK):
        logging.info('Read the validation list from {}'.format(result_fqn))
        validation = mc.read_as_yaml(result_fqn)
        nrao_file_list = validation.get('at_nrao')
        if len(nrao_file_list) > 0:
            logging.info(
                'There are {} NRAO files that are missing from CADC.'.format(
                    len(nrao_file_list)))
            nrao_state_fqn = os.path.join(config.working_directory,
                                          NRAO_STATE)
            logging.info(
                'Look up the URLs for those files in {}'.format(nrao_state_fqn))
            urls_dict = {}
            with open(nrao_state_fqn, 'r') as f:
                for line in f:
                    url = line.split(',')[1]
                    f_name = url.split('/')[-1]
                    urls_dict[f_name] = url
                    logging.error('url {} fname {}'.format(url, f_name))

            logging.info('Build a todo.txt file.')
            with open(config.work_fqn, 'w') as f:
                for f_name in nrao_file_list:
                    logging.error('urls_dict {}'.format(urls_dict.get(f_name)))
                    f.write('{}\n'.format(urls_dict.get(f_name)))
        else:
            logging.info('No NRAO files to retrieve.')
    else:
        logging.error('Need to run validate first, to provide input.')


def validate():
    config = mc.Config()
    config.get_executors()
    caom_list = read_list_from_caom(config)
    nrao_state_fqn = os.path.join(config.working_directory, NRAO_STATE)
    vlass_list = read_list_from_nrao(nrao_state_fqn, config.state_fqn)
    caom = _find_missing(caom_list, vlass_list)
    nrao = _find_missing(vlass_list, caom_list)
    result = {'at_nrao': nrao,
              'at_cadc': caom}
    result_fqn = os.path.join(config.working_directory, VALIDATE_STATE)
    mc.write_as_yaml(result, result_fqn)
    logging.info('There are {} files at NRAO that are not represented '
                 'at CADC, and {} CAOM entries at CADC that are not '
                 'available from NRAO.'.format(len(nrao), len(caom)))
    return nrao, caom


if __name__ == '__main__':
    import sys
    try:
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        validate()
        sys.exit(0)
    except Exception as e:
        logging.error(e)
        logging.debug(traceback.format_exc())
        sys.exit(-1)
