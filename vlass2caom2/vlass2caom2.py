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
import os
import sys
import traceback

from caom2 import Observation
from caom2utils import ObsBlueprint, get_gen_proc_arg_parser
from management import util, CaomExecuteArgPassThrough
from management import decompose_lineage


__all__ = ['main_app', 'update', 'VlassNameHandler', 'VlassArgsPassThrough']

COLLECTION = 'VLASS'
APPLICATION = 'vlass2caom2'


class VlassNameHandler(util.NameHandler):
    def __init__(self, obs_id):
        super(VlassNameHandler, self).__init__(COLLECTION, obs_id)

    def get_file_uri(self):
        """No .gz extension, unlike the default implementation."""
        return 'ad:{}/{}'.format(self.collection, self.get_file_name())

    def get_file_name(self):
        """Already comes with a .fits extension, unlike the default
        implementation."""
        return self.obs_id


def accumulate_wcs(bp):
    """Configure the OMM-specific ObsBlueprint for the CAOM model
    SpatialWCS."""
    logging.debug('Begin accumulate_position.')
    bp.configure_position_axes((1, 2))
    bp.configure_energy_axis(3)
    bp.configure_polarization_axis(4)

    bp.set('Plane.calibrationLevel', '1')
    bp.set('Plane.dataProductType', 'cube')
    bp.set_fits_attribute('Plane.metaRelease', ['DATE-OBS'])


def update(observation, **kwargs):
    """Called to fill multiple CAOM model elements and/or attributes, must
    have this signature for import_module loading and execution.

    :param observation A CAOM Observation model instance.
    :param **kwargs Everything else."""
    logging.debug('Begin update.')

    assert observation, 'non-null observation parameter'
    assert isinstance(observation, Observation), \
        'observation parameter of type Observation'

    for plane in observation.planes:
        for artifact in observation.planes[plane].artifacts:
            for part in observation.planes[plane].artifacts[artifact].parts:
                p = observation.planes[plane].artifacts[artifact].parts[part]
                for chunk in p.chunks:
                    if 'headers' in kwargs:
                        headers = kwargs['headers']

    logging.debug('Done update.')
    return True


class VlassArgsPassThrough(CaomExecuteArgPassThrough):

    def __init__(self):
        super(VlassArgsPassThrough, self).__init__(COLLECTION)

    @staticmethod
    def build_blueprints(artifact_uri):
        """This application relies on the caom2utils fits2caom2 ObsBlueprint
        definition for mapping FITS file values to CAOM model element
        attributes. This method builds the VLASS blueprint for a single
        artifact.

        The blueprint handles the mapping of values with cardinality of 1:1
        between the blueprint entries and the model attributes.

        :param artifact_uri The artifact URI for the file to be processed."""
        blueprint = ObsBlueprint(module=None)
        accumulate_wcs(blueprint)
        blueprints = {artifact_uri: blueprint}
        return blueprints

    @staticmethod
    def build_visit_args(artifact_uri):
        return {'omm_science_file': artifact_uri}

    def build_cardinality(self, args):
        if args.observation:
            fname = args.observation[1]
            observation = args.observation[1]
            uri = VlassNameHandler(fname).get_file_uri()
        if args.lineage:
            fname, uri = decompose_lineage(args.lineage[0])
            observation = fname
        if args.local:
            # TODO this isn't a general VLASS solution
            fname = args.local[0]
            uri = VlassNameHandler(os.path.basename(fname)).get_file_uri()

        kwargs = {'params': {'fname': fname,
                             'collection': self.collection,
                             'observation': observation,
                             'artifact_uri': uri}}
        return kwargs


def main_app():
    args = get_gen_proc_arg_parser().parse_args()
    try:
        VlassArgsPassThrough().collection_augment(args)
    except Exception as e:
        logging.error('Failed {} execution.'.format(APPLICATION))
        logging.error(e)
        tb = traceback.format_exc()
        logging.error(tb)
        sys.exit(-1)

    logging.debug('Done {} processing.'.format(APPLICATION))
