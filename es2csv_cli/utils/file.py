import glob
import os


def purge(filepath):
    filelist = glob.glob(filepath)
    for file in filelist:
        os.remove(file)
