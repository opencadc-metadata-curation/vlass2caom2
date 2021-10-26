from bs4 import BeautifulSoup


def make_list():
    import sys

    with open(sys.argv[1]) as listing:
        soup = BeautifulSoup(listing, 'html.parser')
    file_list = soup.find_all('a', recursive=True)
    suffix = 'I.iter1.image.pbcor.tt0'
    rms_suffix = '{}.rms.subim.fits'.format(suffix)
    subim_suffix = '{}.subim.fits'.format(suffix)
    with open('../../data/rejected_file_names-2018-09-05.csv', 'w') as op:
        for f in file_list:
            href = f.attrs['href'].strip('/')
            if href.startswith('VLASS'):
                fnames = '{}.{}\n{}.{}\n'.format(
                    href, rms_suffix, href, subim_suffix
                )
                op.write(fnames)


if __name__ == "__main__":
    make_list()
