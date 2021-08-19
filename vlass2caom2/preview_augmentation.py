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
#  : 4 $
#
# ***********************************************************************
#

import logging
import os

import matplotlib.image as image
import matplotlib.pyplot as plt
import numpy as np
from astropy.io import fits
from astropy.visualization import ZScaleInterval
from matplotlib.patches import ConnectionPatch, Rectangle

from caom2 import ReleaseType, ProductType
from caom2pipe import manage_composable as mc
from vlass2caom2 import storage_name as sn


class VlassPreview(mc.PreviewVisitor):
    """
    generate_plots code written by Nat Comeau.

    As per Stephen Gwyn suggestion, put whole image in greyscale on left,
    with a zoom-in box on the brightest pixel on the right.
    """

    def __init__(self, observation, **kwargs):
        super(VlassPreview, self).__init__(
            sn.COLLECTION, ReleaseType.META, **kwargs
        )
        self._science_file = self._storage_name.file_name
        self._science_fqn = os.path.join(self._working_dir, self._science_file)
        self._preview_fqn = os.path.join(
            self._working_dir, self._storage_name.prev
        )
        self._thumb_fqn = os.path.join(
            self._working_dir, self._storage_name.thumb)
        self._logger = logging.getLogger(__name__)

    def generate_plots(self, obs_id):
        """Make a preview for a VLASS image. Tested on random sample of 16
        VLASS 1.1, 1.2, 2.1 images."""
        if '.rms.' in self._science_file:
            # there's two files (artifacts) per plane, the non-rms one to
            # generates 'more dense' preview/thumbnail images
            return 0

        self._logger.debug(f'Begin generate_plots for {obs_id}')
        count = 0
        xsize = 200
        ysize = 200

        # Make the figure, roughly twice as wide as tall.
        fig = plt.figure(figsize=(31, 16))
        axs = [fig.add_subplot(1, 2, i + 1) for i in range(2)]

        # Plot the full sized image in the left subplot
        with fits.open(self._science_fqn) as hdul:
            image_data = hdul['PRIMARY'].data[0, 0]
            interval = ZScaleInterval(contrast=0.99)
            array = interval((image_data))
        axs[0].imshow(array, cmap='gray', interpolation='none')
        axs[0].axis('off')

        # Plot the cutout in the right subplot
        with fits.open(self._science_fqn) as hdul:
            cutout_data, x_max, y_max = VlassPreview._get_cutout(
                hdul['PRIMARY'].data[0, 0])
            interval = ZScaleInterval()
            array = interval(cutout_data)
        axs[1].imshow(array, cmap='gray', interpolation='none')
        axs[1].axis('off')

        # Connect the cutout to the full image with two lines
        con = ConnectionPatch(xyA=(y_max + ysize, x_max - xsize), xyB=(0, 1),
                              coordsA="data", coordsB="data",
                              axesA=axs[0], axesB=axs[1], color="#ff0000")
        con.set_linewidth(4)
        axs[1].add_artist(con)
        con = ConnectionPatch(xyA=(y_max + ysize, x_max + xsize), xyB=(0, 398),
                              coordsA="data", coordsB="data",
                              axesA=axs[0], axesB=axs[1], color="#ff0000")
        con.set_linewidth(4)
        axs[1].add_artist(con)

        # Draw a box in the full size image representing the cutout
        rect = Rectangle((y_max - ysize, x_max - xsize), 400, 400, fill=False,
                         edgecolor="red", linewidth=4)
        axs[0].add_patch(rect)
        rect = Rectangle((0, 0), 399, 399, fill=False, edgecolor="red",
                         linewidth=6)
        axs[1].add_patch(rect)

        # Adjust the formatting of the entire figure, and flip to
        # proper orientation.
        fig.suptitle(obs_id, fontsize=36, y=0.92)
        plt.subplots_adjust(wspace=0.01)
        axs[0].invert_yaxis()
        axs[1].invert_yaxis()

        # Scale to appropriate final resolution
        # To decrease/increase the resolution, change the desired_resolution
        # constant here. It doesn't work perfectly, but is close.
        desired_resolution = 1024
        bbox = (
            axs[0]
            .get_window_extent()
            .transformed(fig.dpi_scale_trans.inverted())
        )
        width, height = bbox.width, bbox.height

        # Save as outfile
        plt.savefig(self._preview_fqn, bbox_inches='tight',
                    dpi=int(desired_resolution / height))
        plt.close(fig)
        count += 1
        self.add_preview(self._storage_name.prev_uri, self._storage_name.prev,
                         ProductType.PREVIEW)
        self.add_to_delete(self._preview_fqn)
        count += self._gen_thumbnail()

        self._logger.debug(f'End generate_plots')
        return count

    def _gen_thumbnail(self):
        self._logger.debug(f'Generating thumbnail for file '
                           f'{self._science_fqn}.')
        count = 0
        if os.path.exists(self._preview_fqn):
            thumb = image.thumbnail(self._preview_fqn, self._thumb_fqn,
                                    scale=0.25)
            if thumb is not None:
                self.add_preview(self._storage_name.thumb_uri,
                                 self._storage_name.thumb,
                                 ProductType.THUMBNAIL)
                self.add_to_delete(self._thumb_fqn)
                count = 1
        else:
            self._logger.warning(f'Could not find {self._preview_fqn} for '
                                 f'thumbnail generation.')
        return count

    @staticmethod
    def _get_cutout(array, x_size=200, y_size=200):
        """Tries to find a cutout around the brightest pixel. If this is
        located near the edge of the frame, it selects the next brightest
        pixel and so on for num_tries tries. After that, it falls back to
        just taking a cutout from the centre of the frame. Works by
        progressively masking the highest pixel, second highest pixel, etc...

        array: 2D numpy array

        returns: a 2D subsection of the array.
        """
        num_tries = 200
        masked_array = array.copy()
        masked_array = np.ma.MaskedArray(masked_array)

        for i in range(num_tries):
            # Get x,y coordinate of nth brightest pixel.
            (x_max, y_max) = np.unravel_index(
                np.argmax(masked_array, axis=None), masked_array.shape)
            array_max = masked_array[x_max, y_max]

            y_lower = y_max - y_size
            y_upper = y_max + y_size
            x_lower = x_max - x_size
            x_upper = x_max + x_size

            # If hitting a border of the frame...
            if (
                y_lower < 0
                or x_lower < 0
                or y_upper > masked_array.shape[1]
                or x_upper > masked_array.shape[0]
            ):
                masked_array = np.ma.masked_greater_equal(
                    masked_array, array_max)
                continue

            return (
                array[
                    x_max - x_size : x_max + x_size,
                    y_max - y_size : y_max + y_size,
                ],
                x_max,
                y_max,
            )

        # Fall back to returning a cutout from the centre of the frame.
        x_max = int(array.shape[0] / 2)
        y_max = int(array.shape[1] / 2)
        return (
            array[
                x_max - x_size : x_max + x_size,
                y_max - y_size : y_max + y_size,
            ],
            x_max,
            y_max,
        )


def visit(observation, **kwargs):
    previewer = VlassPreview(observation, **kwargs)
    return previewer.visit(observation)
