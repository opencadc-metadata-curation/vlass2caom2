from astroquery.nrao import Nrao
import astropy.units as u
import astropy.coordinates as coord

result_table = Nrao.query_region(coord.SkyCoord(166.2,76.5,
                                                unit=(u.deg, u.deg)),
                                 radius=2*u.arcmin,
                                 project_code='VLASS1.1')
# result_table = Nrao.query_region("04h33m11.1s 05d21m15.5s")
# result_table = Nrao.query_region("07h54m02s -03d29m59s")

print(len(result_table))
if len(result_table) > 0:
    print(result_table[0])
