#!/bin/bash

wget https://archive-new.nrao.edu/vlass/quicklook/VLASS1.1/QA_REJECTED/
python build_rejected_file_list.py ./index.html
date
exit 0
