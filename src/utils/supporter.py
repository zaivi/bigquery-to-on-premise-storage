import argparse
from datetime import datetime
from src.utils.utils import valid_date



def parse_arg():
    """Parse arguments for a controller

    Returns:
        dict: dictionary containing all parsed arguments
    """
    parser = argparse.ArgumentParser(description='Controller')
    
    # Agruments for migration
    parser.add_argument(
        "-mDate",
        type=valid_date,
        default="N",
        help="The Retrieving Date - format YYYY-MM-DD"
    )
    parser.add_argument(
        '-mTable',
        type=str,
        nargs='+',
        default=['PL_DATA_DETAIL_T5_T6'],
        choices=['PL_DATA_DETAIL_T5_T6', 'PL_DATA_DETAIL_DC', 'DIM_ACCOUNT_CASA', 'DIM_ACCOUNT_LOAN', 'DIM_CDR', "DIM_CUSTOMER", "TRANS_GL"],
        required=True,
        help=''
    )

    # parser.print_help()
    args = parser.parse_args()
    arg_dict = args.__dict__

    return arg_dict

def print_args_info(arg_dict):
    print(f"---------- GETTING DATA FROM GOOGLE CLOUD PLATFORM CONTROLLER ----------")
    print(f"Retrieve Date: {arg_dict['mDate']}")
    print(f"Retrieve Tables: {'None' if arg_dict['mTable'] == ['N'] else ', '.join(arg_dict['mTable'])}")
    print(f"-------------------------------------\n")