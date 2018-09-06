# import os
# import logging
# from bs4 import BeautifulSoup
#
#
# def make_time_csv():
#     import sys
#     import time
#     output_fqn = os.path.join(os.getcwd(),
#                               'vlass_time_data{}.csv'.format(time.time()))
#     with open(output_fqn, 'w') as ofn:
#         for root, subdirs, files in os.walk(sys.argv[1]):
#
#             try:
#                 if len(files) > 0 and 'pipeline' in root and 'html' in root:
#                     logging.error('root {} subdirs {} files{}'.format(
#                         root, subdirs, files))
#                     index_file = os.path.join(root, files[0])
#                     soup = None
#                     with open(index_file) as f:
#                         soup = BeautifulSoup(f, 'html.parser')
#
#                     table = soup.find(summary='Measurement Set Summaries',
#                                       recursive=True)
#                     cells = table.tbody.find_all('tr')[2].find_all('td')
#                     path_bits = root.split('/')
#                     for ii in path_bits:
#                         if ii.startswith('VLASS'):
#                             obs_id = ii.replace('_', '.', 1).split('_')[0]
#                             msg = '{}, {}, {}, {}\n'.format(obs_id,
#                                                          cells[3].get_text(),
#                                                          cells[4].get_text(),
#                                                          cells[5].get_text())
#                             logging.error(msg)
#                             ofn.write(msg)
#             except Exception as e:
#                 logging.error('failed for {}'.format(root))
#
#
# def make_version_csv():
#     import sys
#     import time
#     output_fqn = os.path.join(os.getcwd(),
#                               'vlass_time_data{}.csv'.format(time.time()))
#     with open(output_fqn, 'w') as ofn:
#         for root, subdirs, files in os.walk(sys.argv[1]):
#
#             try:
#                 if len(files) > 0 and 'pipeline' in root and 'html' in root:
#                     logging.error('root {} subdirs {} files{}'.format(
#                         root, subdirs, files))
#                     index_file = os.path.join(root, files[0])
#
#                     soup = None
#                     with open(index_file) as f:
#                         soup = BeautifulSoup(f, 'html.parser')
#
#                     header = soup.find('h2', string='Pipeline Summary',
#                                        recursive=True)
#                     cells = header.parent.find_all('td')
#
#                     table = soup.find(summary='Measurement Set Summaries',
#                                       recursive=True)
#                     ms_cells = table.tbody.find_all('tr')[2].find_all('td')
#
#                     path_bits = root.split('/')
#                     for ii in path_bits:
#                         if ii.startswith('VLASS'):
#                             obs_id = ii.replace('_', '.', 1).split('_')[0]
#                             reference = 'https://{}/index.html'.format(root)
#                             msg = '{}, {}, {}, {}, {}, {}\n'.format(
#                                 obs_id, cells[0].get_text().strip(), reference,
#                                 ms_cells[3].get_text(),
#                                 ms_cells[4].get_text(),
#                                 ms_cells[5].get_text())
#                             logging.error(msg)
#                             ofn.write(msg)
#             except Exception as e:
#                 logging.error('failed for {}'.format(root))
#
#
# if __name__ == "__main__":
#     make_version_csv()
