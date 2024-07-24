__doc__ = """
Colab Utils: a simplified version of Industrial Data Science Workflow for using Colab.

Check the project Github: https://github.com/marcosoares-92/IndustrialDataScienceWorkflow
"""

__author__ = """Marco Cesar Prado Soares; Gabriel Fernandes Luz; Sergio Guilherme Neto"""
__version__ = "1.0.0"


from dataclasses import dataclass

class InvalidInputsError (Exception): pass  # class for raising errors when invalid inputs are provided.


@dataclass
class ControlVars:
    """
    Store the controls for plotting or showing results as Global variables to use.
    Plots and other not-returned elements are saved as ControlVars.

    for a variable var, syntax 
    if var:
        # is equivalent to: "run only when variable var is not None." or "run only if var is True".
    """
    # While show_plots = True, the system will print all plots in functions.
    # While show_results = True, the system prints all results.
    # User must change this variable state to create new connectors.
    show_plots = True
    show_results = True

"""Since these two classes are used by all of the modules, they must be initialized before module import.
If not, a circular import error will be raised, since the modules will try to import two classes that were not created yet.
"""


def copy_to_gcs (file_path, bucket, bucket_folder = None):
    """
    Function for copying a generated file to Google Cloud Storage bucket.
    : param: file_path (str): file path in the root directory where the file is initially saved.
        Examples: "data.csv" will export a file named "data.csv" in the root; "folder/data.csv" will export
        a file in the "folder" directory.
    
    : param: bucket (str): bucket name. Example: "my-bucket" will export to a bucket named my-bucket.
    : param: bucket_folder (str): if the file should be exported to a particular folder or directory of the bucket, declare here.
        If None or empty string, the file will be exported to the main directory of the bucket.
        Examples: "folder" and bucket = "my-bucket" will export to "my-bucket/folder". If "folder/subfolder" will export to
        "my-bucket/folder/subfolder".
    
    return None

    ATTENTION: THIS FUNCTION RUNS IN COLAB BECAUSE gcloud SDK IS ALREADY INSTALLED
    -- gcloud CLI Documentation: https://cloud.google.com/sdk/gcloud/reference
    -- gcloud storage Docs: https://cloud.google.com/sdk/gcloud/reference/storage
    -- gcloud storage cp Doc: https://cloud.google.com/sdk/gcloud/reference/storage/cp
    """

    import os
    from subprocess import Popen, PIPE, TimeoutExpired

    # Obtain path on Google Cloud Storage
    gcs_path = f"gs://{bucket}"

    if ((bucket_folder is not None) & (len(str(bucket_folder)) > 0)):
        gcs_path = os.path.join(gcs_path, bucket_folder)
    
    cmd_line = f"""gcloud storage cp {file_path} {gcs_path}"""
    # This command access GCS through gcloud CLI and copies the file in {file_path} to the bucket specified in {gcs_path}.
    # One could try in IPython: ! gcloud storage cp {file_path} {gcs_path}

    proc = Popen(cmd_line.split(" "), stdout = PIPE, stderr = PIPE)
    """cmd_line = "gcloud storage cp {file_path} {gcs_path}"
      will lead to the list ['gcloud', 'storage', 'cp', '{file_path}', '{gcs_path}']
      after splitting the string in whitespaces, what is done by .split(" ") method.
    """
    try:
        output, error = proc.communicate(timeout = 60) # give up after 60 seconds
        print(f"Copying file {file_path} to {gcs_path}")
        if len(msg > 0):
          print (msg)
          
    except:
        # General exception
        output, error = proc.communicate()


from .datafetch import *


