import sys
import os

if __name__ == "__main__":
    sys.path.append('/Users/felipemeloc/Documents/jobsity_challenge/jobsity/ingest_data')
    from ingest_data import ingest_data
    ingest_data.main_ingest()
