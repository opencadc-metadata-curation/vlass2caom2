from astropy.io import fits
from astropy import wcs
import copy

fn = '/app/test_data/alma/AT20GJ080633-711217.StokesIQU.clean.image.fits'

fh = fits.open(fn)
print(len(fh))
fh[0].header['ORIGIN'] = 'CASA 4.7.0-REL (r38335)'
print('Header origin: "{0}"'.format(fh[0].header['ORIGIN']))
# header = copy.copy(fh[0].header)
header = copy.copy(fh[0])
header.header['ORIGIN'] = 'BLANK'
header.header.update()
# with open('/app/tmp.fits', 'wb') as f:
#     f.write(header)
# try:
#     # print(wcs.WCS(header))
#     # fits.writeto('/app/tmp.fits.header', None, header)
# except ValueError:
#     print(wcs.WCS(header))


data, ignore = fits.getdata(fn, header=True)

# for ii in range(0, 2):
#     header = fits.getheader(fn, ii)
#     header['ORIGIN'] = 'CASA 4.7.0-REL (r38335)'
#     print('Header origin: "{0}"'.format(header['ORIGIN']))
#     header2 = copy.copy(header)
#     header2['ORIGIN'] = 'BLANK'
#     header2.update()

for ii in header.header.items():
    if ii[0] == 'HISTORY':
        continue
    print('{}={}'.format(ii[0], ii[1]))

# header.writeto('/app/tmp.fits.header', overwrite=True)
