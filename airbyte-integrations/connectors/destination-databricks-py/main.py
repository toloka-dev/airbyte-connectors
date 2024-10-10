#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from destination_databricks_py import DestinationDatabricks

if __name__ == "__main__":
    DestinationDatabricks().run(sys.argv[1:])
