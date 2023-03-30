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

from caom2pipe import astro_composable as ac
from caom2pipe.manage_composable import Features, read_obs_from_file, StorageName, write_obs_to_file
from caom2pipe import reader_composable as rdc
from vlass2caom2 import storage_name, fits2caom2_augmentation
from caom2.diff import get_differences

import os
import pytest

from mock import patch


THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TEST_DATA_DIR = os.path.join(THIS_DIR, 'data')
PLUGIN = os.path.join(os.path.dirname(THIS_DIR), 'main_app.py')
a = 'VLASS1.1.ql.T01t01.J000228-363000.10.2048.v1.I.iter1.image.pbcor.tt0.rms.subim.fits.header'
b = 'VLASS1.1.ql.T01t01.J000228-363000.10.2048.v1.I.iter1.image.pbcor.tt0.subim.fits.header'
c = 'VLASS1.1.ql.T10t12.J075402-033000.10.2048.v1.I.iter1.image.pbcor.tt0.rms.subim.fits.header'
d = 'VLASS1.1.ql.T10t12.J075402-033000.10.2048.v1.I.iter1.image.pbcor.tt0.subim.fits.header'
e = 'VLASS1.1.ql.T29t05.J110448+763000.10.2048.v1.I.iter1.image.pbcor.tt0.rms.subim.fits.header'
f = 'VLASS1.1.ql.T29t05.J110448+763000.10.2048.v1.I.iter1.image.pbcor.tt0.subim.fits.header'
g = 'VLASS1.1.cat.T29t05.J110448+763000.10.2048.v1.csv'
h = 'VLASS1.1.cc.T29t05.J110448+763000.10.2048.v1.fits.header'
i = 'VLASS1.2.ql.T07t14.J084202-123000.10.2048.v1.I.iter1.image.pbcor.tt0.rms.subim.fits.header'
j = 'VLASS1.2.ql.T07t14.J084202-123000.10.2048.v1.I.iter1.image.pbcor.tt0.subim.fits.header'
k = 'VLASS2.1.se.T10t01.J000200-003000.06.2048.v1.I.iter3.alpha.error.subim.fits.header'
l = 'VLASS2.1.se.T10t01.J000200-003000.06.2048.v1.I.iter3.alpha.subim.fits.header'
m = 'VLASS2.1.se.T10t01.J000200-003000.06.2048.v1.I.iter3.image.pbcor.tt0.rms.subim.fits.header'
n = 'VLASS2.1.se.T10t01.J000200-003000.06.2048.v1.I.iter3.image.pbcor.tt0.subim.fits.header'
o = 'VLASS2.1.se.T10t01.J000200-003000.06.2048.v1.I.iter3.image.pbcor.tt1.rms.subim.fits.header'
p = 'VLASS2.1.se.T10t01.J000200-003000.06.2048.v1.I.iter3.image.pbcor.tt1.subim.fits.header'
q = 'VLASS2.1.se.T10t01.J000200-003000.06.2048.v1.I.catalog.csv'
r = 'VLASS1.1.ql.T06t24.J152614-163000.10.2048.v1.I.iter1.image.pbcor.tt0.rms.subim.fits.header'
s = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.V_mean_minmaxrej.subim.fits.header'
t = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw10.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
u = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw10.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
# v = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw10.IQU.iter3.image.pbcor.tt0.subim.fits.header'
w = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw11.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
x = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw11.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
# y = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw11.IQU.iter3.image.pbcor.tt0.subim.fits.header'
z = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw12.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
aa = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw12.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
# ab = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw12.IQU.iter3.image.pbcor.tt0.subim.fits.header'
be = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw13.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
ad = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw13.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
# ae = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw13.IQU.iter3.image.pbcor.tt0.subim.fits.header'
af = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw14.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
ag = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw14.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
# ah = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw14.IQU.iter3.image.pbcor.tt0.subim.fits.header'
ai = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw15.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
aj = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw15.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
# ak = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw15.IQU.iter3.image.pbcor.tt0.subim.fits.header'
al = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw2.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
am = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw2.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
# an = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw2.IQU.iter3.image.pbcor.tt0.subim.fits.header'
ao = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw5.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
ap = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw5.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
# aq = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw5.IQU.iter3.image.pbcor.tt0.subim.fits.header'
ar = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw6.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
au = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw6.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
# av = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw6.IQU.iter3.image.pbcor.tt0.subim.fits.header'
aw = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw7.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
# ax = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw7.IQU.iter3.image.pbcor.tt0.subim.fits.header'
ay = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw8.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
az = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw8.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
# ba = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw8.IQU.iter3.image.pbcor.tt0.subim.fits.header'
bb = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw9.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
bc = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw9.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
# bd = 'VLASS2.1.cc.T10t02.J005000-023000.06.2048.v1.spw9.IQU.iter3.image.pbcor.tt0.subim.fits.header'

da = 'VLASS2.1.cc.T10t35.J230600-003000.06.2048.v1.spw10.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
db = 'VLASS2.1.cc.T10t35.J230600-003000.06.2048.v1.spw10.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
dc = 'VLASS2.1.cc.T10t35.J230600-003000.06.2048.v1.spw11.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
dd = 'VLASS2.1.cc.T10t35.J230600-003000.06.2048.v1.spw11.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
de = 'VLASS2.1.cc.T10t35.J230600-003000.06.2048.v1.spw12.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
df = 'VLASS2.1.cc.T10t35.J230600-003000.06.2048.v1.spw12.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
dg = 'VLASS2.1.cc.T10t35.J230600-003000.06.2048.v1.spw13.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
dh = 'VLASS2.1.cc.T10t35.J230600-003000.06.2048.v1.spw13.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
di = 'VLASS2.1.cc.T10t35.J230600-003000.06.2048.v1.spw14.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
dj = 'VLASS2.1.cc.T10t35.J230600-003000.06.2048.v1.spw14.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
dk = 'VLASS2.1.cc.T10t35.J230600-003000.06.2048.v1.spw15.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
dl = 'VLASS2.1.cc.T10t35.J230600-003000.06.2048.v1.spw15.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
dm = 'VLASS2.1.cc.T10t35.J230600-003000.06.2048.v1.spw2.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
dn = 'VLASS2.1.cc.T10t35.J230600-003000.06.2048.v1.spw2.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
do = 'VLASS2.1.cc.T10t35.J230600-003000.06.2048.v1.spw5.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
dp = 'VLASS2.1.cc.T10t35.J230600-003000.06.2048.v1.spw5.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
dq = 'VLASS2.1.cc.T10t35.J230600-003000.06.2048.v1.spw6.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
dr = 'VLASS2.1.cc.T10t35.J230600-003000.06.2048.v1.spw6.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
ds = 'VLASS2.1.cc.T10t35.J230600-003000.06.2048.v1.spw7.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
dt = 'VLASS2.1.cc.T10t35.J230600-003000.06.2048.v1.spw7.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
du = 'VLASS2.1.cc.T10t35.J230600-003000.06.2048.v1.spw8.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
dv = 'VLASS2.1.cc.T10t35.J230600-003000.06.2048.v1.spw8.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
dw = 'VLASS2.1.cc.T10t35.J230600-003000.06.2048.v1.spw9.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
dx = 'VLASS2.1.cc.T10t35.J230600-003000.06.2048.v1.spw9.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'


ga = 'VLASS2.1.cc.T10t29.J184200-033000.06.2048.V_mean_minmaxrej.subim.fits.header'
gb = 'VLASS2.1.cc.T10t29.J184200-033000.06.2048.v1.spw2.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
gc = 'VLASS2.1.cc.T10t29.J184200-033000.06.2048.v1.spw10.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
gd = 'VLASS2.1.cc.T10t29.J184200-033000.06.2048.v1.spw2.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
ge = 'VLASS2.1.cc.T10t29.J184200-033000.06.2048.v1.spw10.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
gf = 'VLASS2.1.cc.T10t29.J184200-033000.06.2048.v1.spw5.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
gg = 'VLASS2.1.cc.T10t29.J184200-033000.06.2048.v1.spw11.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
gh = 'VLASS2.1.cc.T10t29.J184200-033000.06.2048.v1.spw5.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
gi = 'VLASS2.1.cc.T10t29.J184200-033000.06.2048.v1.spw11.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
gj = 'VLASS2.1.cc.T10t29.J184200-033000.06.2048.v1.spw6.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
gk = 'VLASS2.1.cc.T10t29.J184200-033000.06.2048.v1.spw12.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
gl = 'VLASS2.1.cc.T10t29.J184200-033000.06.2048.v1.spw6.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
gm = 'VLASS2.1.cc.T10t29.J184200-033000.06.2048.v1.spw12.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
gn = 'VLASS2.1.cc.T10t29.J184200-033000.06.2048.v1.spw7.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
go = 'VLASS2.1.cc.T10t29.J184200-033000.06.2048.v1.spw13.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
gp = 'VLASS2.1.cc.T10t29.J184200-033000.06.2048.v1.spw7.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
gq = 'VLASS2.1.cc.T10t29.J184200-033000.06.2048.v1.spw13.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
gr = 'VLASS2.1.cc.T10t29.J184200-033000.06.2048.v1.spw8.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
gs = 'VLASS2.1.cc.T10t29.J184200-033000.06.2048.v1.spw14.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
gt = 'VLASS2.1.cc.T10t29.J184200-033000.06.2048.v1.spw8.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
gu = 'VLASS2.1.cc.T10t29.J184200-033000.06.2048.v1.spw14.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
gv = 'VLASS2.1.cc.T10t29.J184200-033000.06.2048.v1.spw9.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
gw = 'VLASS2.1.cc.T10t29.J184200-033000.06.2048.v1.spw15.IQU.iter3.image.pbcor.tt0.rms.subim.fits.header'
gx = 'VLASS2.1.cc.T10t29.J184200-033000.06.2048.v1.spw9.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'
gy = 'VLASS2.1.cc.T10t29.J184200-033000.06.2048.v1.spw15.IQU.iter3.image.pbcor.tt0.subim.com.fits.header'

obs_id_a = 'VLASS1.1.T01t01.J000228-363000'
obs_id_c = 'VLASS1.1.T10t12.J075402-033000'
obs_id_e = 'VLASS1.1.T29t05.J110448+763000'
obs_id_f = 'VLASS1.2.T07t14.J084202-123000'
obs_id_k = 'VLASS2.1.T10t01.J000200-003000'
obs_id_r = 'VLASS1.1.T06t24.J152614-163000'
obs_id_s = 'VLASS2.1.T10t02.J005000-023000'
obs_id_d = 'VLASS2.1.T10t35.J230600-003000'
obs_id_g = 'VLASS2.1.T10t29.J184200-033000'

test_obs = [
    [obs_id_a, a, b],
    [obs_id_c, c, d],
    [obs_id_c + 'r', c.replace('v1', 'v2'), d.replace('v1', 'v2')],
    [obs_id_f, i, j],
    [obs_id_k, k, l, m, n, o, p, q],
    [obs_id_r, r],
    # [obs_id_s, t, u, v, w, x, y, z, aa, ab, be, ad, ae, af, ag, ah, ai, aj, ak, al, am, an, ao, ap, aq, ar, au,
    #  av, aw, ax, ay, az, ba, bb, bc, bd],
    [obs_id_s, t, u, w, x, z, aa, be, ad, af, ag, ai, aj, al, am, ao, ap, ar, au, aw, ay, az, bb, bc],
    [obs_id_d, da, db, dc, dd, de, df, dg, dh, di, dj, dk, dl, dm, dn, do, dp, dq, dr, ds, dt, du, dv, dw, dx],
    # no minmaxrej file in the 'g' list
    [obs_id_g, gb, gc, gd, ge, gf, gg, gh, gi, gj, gk, gl, gm, gn, go, gp, gq, gr, gs, gt, gu, gv, gw, gx],
]


@pytest.mark.parametrize('test_files', test_obs)
@patch('caom2utils.data_util.get_local_headers_from_fits')
def test_visit(header_mock, test_files, test_config):
    obs_id = test_files[0]
    header_mock.side_effect = ac.make_headers_from_file
    expected_fqn = f'{TEST_DATA_DIR}/{obs_id}.expected.xml'
    expected = None
    if os.path.exists(expected_fqn):
        expected = read_obs_from_file(expected_fqn)
    in_fqn = expected_fqn.replace('expected.xml', 'in.xml')
    actual_fqn = expected_fqn.replace('expected.xml', 'actual.xml')
    if os.path.exists(actual_fqn):
        os.unlink(actual_fqn)

    observation = None
    if os.path.exists(in_fqn):
        observation = read_obs_from_file(in_fqn)

    for f_name in test_files[1:]:
        temp_fqn = f'{TEST_DATA_DIR}/{f_name}'
        vlass_name = storage_name.VlassName(entry=temp_fqn)
        metadata_reader = rdc.FileMetadataReader()
        metadata_reader.set(vlass_name)
        file_type = 'application/fits'
        metadata_reader.file_info[vlass_name.file_uri].file_type = file_type
        kwargs = {
            'storage_name': vlass_name,
            'metadata_reader': metadata_reader,
        }
        observation = fits2caom2_augmentation.visit(observation, **kwargs)
    try:
        if expected is None:
            write_obs_to_file(observation, actual_fqn)
            assert False, f'{observation.observation_id} no expected to compare to'
        else:
            compare_result = get_differences(expected, observation)
    except Exception as exc:
        write_obs_to_file(observation, actual_fqn)
        raise exc
    if compare_result is not None:
        write_obs_to_file(observation, actual_fqn)
        compare_text = '\n'.join([r for r in compare_result])
        msg = f'Differences found in observation {observation.observation_id}\n' f'{compare_text}'
        assert False, msg
    # assert False  # cause I want to see logging messages
