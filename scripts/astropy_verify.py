from astropy.io import fits

def verify(this_file):
    hdulist = fits.open(this_file, memmap=True, lazy_load_hdus=False)
    hdulist.verify('warn')
    for h in hdulist:
      h.verify('warn')
    hdulist.close()
    

if __name__ == "__main__":
    import sys
    arg1 = sys.argv[1]
    verify(arg1)
