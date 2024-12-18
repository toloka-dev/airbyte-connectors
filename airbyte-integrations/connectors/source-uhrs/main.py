import sys

from airbyte_cdk.entrypoint import launch
from source_uhrs import SourceUhrs

if __name__ == "__main__":
    source = SourceUhrs()
    launch(source, sys.argv[1:])
