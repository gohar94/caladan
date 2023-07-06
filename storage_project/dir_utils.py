# -*- coding: utf-8 -*-

import os


class cd:
    """
    Context manager for changing the current working directory

    Source: https://stackoverflow.com/a/13197763/5937661
    """

    def __init__(self, new_dir_path):
        self.new_dir_path = os.path.expanduser(new_dir_path)

    def __enter__(self):
        self.saved_path = os.getcwd()
        os.chdir(self.new_dir_path)

    def __exit__(self, etype, value, traceback):
        os.chdir(self.saved_path)
