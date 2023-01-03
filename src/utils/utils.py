import os
import shutil
import random
import string
import argparse
import datetime
import pandas as pd


def delete_folder(folder_name):
    if os.path.exists(folder_name):
        shutil.rmtree(folder_name)


def read_csv(file_name):
    df = pd.read_csv(file_name)
    list_values = df.values.tolist()
    print(df.info(memory_usage='deep'))

    return list_values


def valid_date(str_date):
    try:
        if str_date.lower() == "none" or str_date.lower() == "n":
            return "None"
        return datetime.datetime.strptime(str_date, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        msg = "not a valid date: {0!r}".format(str_date)
        raise argparse.ArgumentTypeError(msg)


def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))