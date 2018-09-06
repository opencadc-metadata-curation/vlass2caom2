from caom2pipe import manage_composable as mc


def remove_dups():
    fqn = '/usr/src/app/vlass2caom2/data/ArchiveQuery-2018-08-15.csv'
    content = mc.read_csv_file(fqn)
    skinny_content = {}
    for ii in content:
        key = ii[0]
        skinny_content[key] = ii

    with open('./test.csv', 'w') as f:
        for ii in skinny_content:
            f.write('{}\n'.format(','.join(skinny_content[ii])))


if __name__ == "__main__":
    remove_dups()
