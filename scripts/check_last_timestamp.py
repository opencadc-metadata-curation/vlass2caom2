import logging
from vlass2caom2 import scrape
logger = logging.getLogger()
logger.setLevel(logging.INFO)
scrape.query_top_page()
