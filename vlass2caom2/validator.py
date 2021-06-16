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

from collections import defaultdict
from dataclasses import dataclass
from dateutil import tz
from urllib import parse as parse

from astropy.io.votable import parse_single_table

from cadcutils import net
from cadctap import CadcTapClient
from caom2repo import CAOM2RepoClient
from caom2pipe import client_composable as clc
from caom2pipe import manage_composable as mc

from vlass2caom2 import APPLICATION, scrape, VlassName, COLLECTION

__all__ = ['VlassValidator', 'validate']

NRAO_STATE = 'nrao_state.yml'
ISO_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'


@dataclass
class FileMeta:
    """
    The information that is used to determine which version of a file
    at NRAO to keep at CADC, since CADC keeps only the latest version.
    """

    url: str
    version: str
    dt: str
    f_name: str


def read_file_list_from_archive(config):
    ad_resource_id = 'ivo://cadc.nrc.ca/ad'
    agent = f'{APPLICATION}/1.0'
    subject = net.Subject(certificate=config.proxy_fqn)
    client = net.BaseWsClient(
        resource_id=ad_resource_id,
        subject=subject,
        agent=agent,
        retry=True,
    )
    query_meta = (
        f"SELECT fileName FROM archive_files WHERE archiveName = "
        f"'{config.archive}'"
    )
    data = {'QUERY': query_meta, 'LANG': 'ADQL', 'FORMAT': 'csv'}
    logging.debug(f'Query is {query_meta}')
    try:
        response = client.get(
            f'https://{client.host}/ad/sync?{parse.urlencode((data))}',
            cert=config.proxy_fqn,
        )
        if response.status_code == 200:
            # ignore the column name as the first part of the response
            artifact_files_list = response.text.split()[1:]
            return artifact_files_list
        else:
            raise mc.CadcException(f'Query failure {response}')
    except Exception as e:
        raise mc.CadcException(f'Failed ad content query: {e}')


def read_list_from_caom(config):
    query = (
        f"SELECT A.uri FROM caom2.Observation AS O "
        f"JOIN caom2.Plane AS P ON O.obsID = P.obsID "
        f"JOIN caom2.Artifact AS A ON P.planeID = A.planeID "
        f"WHERE O.collection='{config.archive}'"
    )
    subject = net.Subject(certificate=config.proxy_fqn)
    tap_client = CadcTapClient(subject, resource_id=config.tap_id)
    buffer = io.BytesIO()
    tap_client.query(query, output_file=buffer)
    temp = parse_single_table(buffer).to_table()
    return [
        ii.decode().replace(f'ad:{config.archive}/', '').strip()
        for ii in temp['uri']
    ]


def read_list_from_nrao(nrao_state_fqn):
    if os.path.exists(nrao_state_fqn):
        vlass_list = mc.read_as_yaml(nrao_state_fqn)
    else:
        start_date = scrape.make_date_time('01Jan1990 00:00')
        vlass_list, vlass_date = scrape.build_file_url_list(start_date)
        mc.write_as_yaml(vlass_list, nrao_state_fqn)
    result = {}
    validate_dict = {}
    for key, value in vlass_list.items():
        for url in value:
            f_name = url.split('/')[-1]
            result[f_name] = key
            validate_dict[f_name] = url
    return result, validate_dict


def read_file_url_list_from_nrao(nrao_state_fqn):
    """
    :param nrao_state_fqn: str cache file name
    :return: result dict key is file_name, value is timestamp from NRAO site
        of file
        validate_dict key is file_name, value is NRAO URL of file
    """
    if os.path.exists(nrao_state_fqn):
        vlass_list = mc.read_as_yaml(nrao_state_fqn)
    else:
        start_date = scrape.make_date_time('01Jan1990 00:00')
        vlass_list = scrape.build_url_list(start_date)
        mc.write_as_yaml(vlass_list, nrao_state_fqn)
    result, validate_dict = get_file_url_list_max_versions(vlass_list)
    return result, validate_dict


def get_file_url_list_max_versions(nrao_dict):
    """
    SG - 22-10-20 - ignore old versions for validation

    :param nrao_dict: dict, keys are NRAO URLs, values are seconds since epoch
        timestamp of the URL at NRAO
    :return:
    """
    obs_id_dict = defaultdict(list)
    # Identify the observation IDs, which are version in-sensitive. This will
    # have the effect of building up a list of URLs of all versions by
    # observation ID
    for key, value in nrao_dict.items():
        storage_name = VlassName(key)
        file_meta = FileMeta(
            key, storage_name.version, value, storage_name.file_name
        )
        obs_id_dict[storage_name.obs_id].append(file_meta)

    # now handle the URLs based on how many there are per observation ID
    result = {}
    validate_dict = {}
    for key, value in obs_id_dict.items():
        if len(value) <= 2:
            # remove the fully-qualified path names from the validator list
            # while creating a dictionary where the file name is the key, and
            # the fully-qualified file name at NRAO is the value
            #
            for entry in value:
                result[entry.f_name] = entry.dt
                validate_dict[entry.f_name] = entry.url
        else:
            max_version = _get_max_version(value)
            for entry in value:
                if max_version == entry.version:
                    result[entry.f_name] = entry.dt
                    validate_dict[entry.f_name] = entry.url
                else:
                    logging.debug(f'Old version:: {entry.f_name}')
    return result, validate_dict


def _get_max_version(entries):
    max_version = 1
    for entry in entries:
        max_version = max(max_version, entry.version)
    return max_version


class VlassValidator(mc.Validator):
    def __init__(self):
        super(VlassValidator, self).__init__(
            source_name='NRAO', source_tz=tz.gettz('Canada/Eastern')
        )
        # a dictionary where the file name is the key, and the fully-qualified
        # file name at the HTTP site is the value
        self._fully_qualified_list = None

    def read_from_source(self):
        nrao_state_fqn = os.path.join(
            self._config.working_directory, NRAO_STATE
        )
        validator_list, fully_qualified_list = read_file_url_list_from_nrao(
            nrao_state_fqn)
        self._fully_qualified_list = fully_qualified_list
        return validator_list

    def write_todo(self):
        with open(self._config.work_fqn, 'w') as f:
            for entry in self._source:
                f.write(f'{self._fully_qualified_list[entry]}\n')
            for entry in self._destination_data:
                f.write(f'{self._fully_qualified_list[entry]}\n')

    def _filter_result(self):
        config = mc.Config()
        config.get_executors()
        subject = clc.define_subject(config)
        caom_client = CAOM2RepoClient(subject)
        metrics = mc.Metrics(config)
        for entry in self._source:
            if VlassValidator._later_version_at_cadc(
                entry, caom_client, metrics
            ):
                self._source.remove(entry)

    @staticmethod
    def _later_version_at_cadc(entry, caom_client, metrics):
        later_version_found = False
        storage_name = VlassName(entry)
        caom_at_cadc = clc.repo_get(
            caom_client, COLLECTION, storage_name.obs_id, metrics
        )
        if caom_at_cadc is not None:
            for plane in caom_at_cadc.planes.values():
                for artifact in plane.artifacts.values():
                    if 'jpg' in artifact.uri:
                        continue
                    (
                        ignore_scheme,
                        ignore_collection,
                        f_name,
                    ) = mc.decompose_uri(artifact.uri)
                    vlass_name = VlassName(f_name)
                    if vlass_name.version > storage_name.version:
                        # there's a later version at CADC, everything is good
                        # ignore the failure report
                        later_version_found = True
                        break
                if later_version_found:
                    break
        return later_version_found


def validate():
    validator = VlassValidator()
    validator.validate()
    validator.write_todo()


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
