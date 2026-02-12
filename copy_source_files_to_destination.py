import logging
from argparse import ArgumentParser, Namespace

from ops_utils.gcp_utils import GCPCloudFunctions

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

def get_args() -> Namespace:
    """Parse command line arguments."""
    parser = ArgumentParser(description="Copy source files to destination buckets with validation")
    parser.add_argument("--source_destination_tsv",help="The GCP path to the source/destination mapping tsv")
    return parser.parse_args()

if __name__ == '__main__':
    args = get_args()

    # Copy sequencing files to their destination buckets
    GCPCloudFunctions().multithread_copy_of_files_with_validation(files_to_copy=args.source_destination_tsv)
