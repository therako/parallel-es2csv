import glob
import os


def purge(filepath):
    filelist = glob.glob(filepath)
    for file in filelist:
        if os.path.isdir(file):
            sub_folder_files = os.listdir(file)
            [os.remove(os.path.join(file, x)) for x in sub_folder_files]
        else:
            os.remove(file)
