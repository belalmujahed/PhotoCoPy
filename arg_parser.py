import argparse

class CopyArgParser:

    def __init__(self):
        self.args = self.parse_args()

    def parse_args(self):
        parser = argparse.ArgumentParser()

        parser.add_argument("-s", "--source", help="The source directory", required=True)
        parser.add_argument("-d", "--destinations", help="The destination directory/ies",  required=True, nargs="*")
        parser.add_argument("-i", "--ignore", help="The file extensions to ignore",  required=False)
        parser.add_argument("-b", "--buffer", help="The directory where a temporary buffer will be created. Will reduce dependence on slow source directory. Ideal to use an SSD for this buffer", required=False)

        return parser.parse_args()

