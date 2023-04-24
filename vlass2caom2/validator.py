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

import logging
import os
import traceback

import pandas as pd

from collections import defaultdict
from dataclasses import dataclass

from caom2pipe import client_composable as clc
from caom2pipe import manage_composable as mc
from caom2pipe.validator_composable import Validator

from vlass2caom2 import data_source, storage_name

__all__ = ['NRAO_STATE', 'VlassValidator', 'validate']

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


def read_file_url_list_from_nrao(nrao_state_fqn):
    """
    :param nrao_state_fqn: str cache file name
    :return: pandas DataFrame, columns url, timestamp
            url is NRAO URL of file
            timestamp is from NRAO site of file
    """
    logging.debug(f'Begin read_file_url_list_from_nrao with {nrao_state_fqn}')
    if os.path.exists(nrao_state_fqn):
        vlass_dict = pd.read_csv(nrao_state_fqn)
    else:
        config = mc.Config()
        config.get_executors()
        session = mc.get_endpoint_session()
        source = data_source.NraoPages(config, session)
        temp = source.get_all_file_urls()
        vlass_dict = pd.DataFrame({'url': temp.keys(), 'dt': temp.values()})
        vlass_dict.to_csv(nrao_state_fqn, header=True, index=False)
    logging.debug('End read_file_url_list_from_nrao.')
    return vlass_dict


def get_file_url_list_max_versions(nrao_dict):
    """
    SG - 22-10-20 - ignore old versions for validation

    :param nrao_dict: dict, keys are NRAO URLs, values are datetime of the URL at NRAO
    :return: result dict key : file name, value: datetime
             validate_dict dict key: file name, value: url
    """
    obs_id_dict = defaultdict(list)
    # Identify the observation IDs, which are version in-sensitive. This will
    # have the effect of building up a list of URLs of all versions by
    # observation ID
    for key, value in nrao_dict.items():
        vlass_name = storage_name.VlassName(key)
        file_meta = FileMeta(key, vlass_name.version, value, vlass_name.file_name)
        obs_id_dict[vlass_name.obs_id].append(file_meta)

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


class VlassValidator(Validator):
    def __init__(self):
        super(VlassValidator, self).__init__(source_name='NRAO')
        # a dictionary where the file name is the key, and the fully-qualified
        # file name at the HTTP site is the value
        self._fully_qualified_list = None
        self._artifact_uris = pd.DataFrame()

    def read_from_source(self):
        self._logger.debug('Begin read_from_source')
        nrao_state_fqn = os.path.join(self._config.working_directory, NRAO_STATE)
        # columns are url, dt
        temp = read_file_url_list_from_nrao(nrao_state_fqn)
        temp['f_name'] = temp.url.apply(Validator.filter_column)
        # columns are url, dt, f_name
        self._logger.debug('End read_from_source')
        return temp

    def write_todo(self):
        source_fqn = self._config.work_fqn.replace('todo', 'store_todo')
        self._source_missing['url'].to_csv(source_fqn, header=False, index=False)
        destination_fqn = self._config.work_fqn.replace('todo', 'ingest_todo')
        self._destination_older['f_name'].to_csv(destination_fqn, header=False, index=False)

    def _filter_result(self, dest_meta_temp):
        # self._version_check_query()
        dest_meta_temp['version'] = dest_meta_temp.uri.apply(VlassValidator.get_version)
        dest_meta_temp['common_bits'] = dest_meta_temp.uri.apply(VlassValidator.get_common_bits)
        # how to figure out if there's more than one version?

    def _version_check_query(self):
        q = f"""
        SELECT O.observationID, A.uri
        FROM caom2.Observation AS O
        JOIN caom2.Plane AS P ON P.obsID = O.obsID
        JOIN caom2.Artifact AS A ON A.planeID = P.planeID
        WHERE O.collection = '{self._config.collection}'
        AND A.uri like '%fits'
        """
        self._artifact_uris = mc.query_tap_pandas(q, self._caom_client)

    def _later_version_at_cadc(self, entry, caom_client, metrics):
        later_version_found = False
        vlass_name = storage_name.VlassName(entry)
        caom_at_cadc = clc.repo_get(caom_client, self._config.collection, vlass_name.obs_id, metrics)
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
                    vlass_name = storage_name.VlassName(f_name)
                    if vlass_name.version > storage_name.version:
                        # there's a later version at CADC, everything is good
                        # ignore the failure report
                        later_version_found = True
                        break
                if later_version_found:
                    break
        return later_version_found

    @staticmethod
    def get_common_bits(a):
        bits = a.split('.')
        return '.'.join(ii for ii in bits if not ii.startswith('v') and not ii.startswith('iter'))

    @staticmethod
    def get_version(a):
        return storage_name.VlassName.extract_version(a)


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
