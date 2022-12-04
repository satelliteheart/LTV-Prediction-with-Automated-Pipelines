from leaf import process_db_challenges, process_db_stores, process_funnel, process_mixpanel
from tools import cleanup_data

if "__main__" == __name__:
    cleanup_data()
    process_db_stores()
    process_db_challenges()
    process_funnel()
    process_mixpanel()
