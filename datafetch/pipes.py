import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from colab-utils import (InvalidInputsError, ControlVars)
from .core import (Connectors, MountGoogleDrive, SQLServerConnection, 
                    SQLiteConnection, GCPBigQueryConnection)



def mount_storage_system (source = 'google', path_to_store_imported_s3_bucket = '', s3_bucket_name = None, s3_obj_prefix = None):
    """
    mount_storage_system (source = 'aws', path_to_store_imported_s3_bucket = '', s3_bucket_name = None, s3_obj_prefix = None):

    : param: source = 'google' for mounting the google drive;
    : param: source = 'aws' for mounting an AWS S3 bucket.
    
    THE FOLLOWING PARAMETERS HAVE EFFECT ONLY WHEN source == 'aws'
    
    : param: path_to_store_imported_s3_bucket: path of the Python environment to which the
    : param: S3 bucket contents will be imported. If it is None, or if 
    : param: path_to_store_imported_s3_bucket = '/', bucket will be imported to the root path. 
    : param: Alternatively, input the path as a string (in quotes). e.g. 
    : param: path_to_store_imported_s3_bucket = 'copied_s3_bucket'
    
    : param: s3_bucket_name = None.
      This parameter is obbligatory to access an AWS S3 bucket. Substitute it for a string
      with the bucket's name. e.g. s3_bucket_name = "aws-bucket-1" access a bucket named as
      "aws-bucket-1"
    
    : param: s3_obj_prefix = None. Keep it None or as an empty string (s3_obj_key_prefix = '')
      to import the whole bucket content, instead of a single object from it.
      Alternatively, set it as a string containing the subfolder from the bucket to import:
      Suppose that your bucket (admin-created) has four objects with the following object 
      keys: Development/Projects1.xls; Finance/statement1.pdf; Private/taxdocument.pdf; and
      s3-dg.pdf. The s3-dg.pdf key does not have a prefix, so its object appears directly 
      at the root level of the bucket. If you open the Development/ folder, you see 
      the Projects.xlsx object in it.
      Check Amazon documentation:
      https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
    
      In summary, if the path of the file is: 'bucket/my_path/.../file.csv'
      where 'bucket' is the bucket's name, key_prefix = 'my_path/.../', without the
      'file.csv' (file name with extension) last part.
    
      So, declare the prefix as S3_OBJECT_FOLDER_PREFIX to import only files from
      a given folder (directory) of the bucket.
      DO NOT PUT A SLASH before (to the right of) the prefix;
      DO NOT ADD THE BUCKET'S NAME TO THE right of the prefix:
      S3_OBJECT_FOLDER_PREFIX = "bucket_directory1/.../bucket_directoryN/"

      Alternatively, provide the full path of a given file if you want to import only it:
      S3_OBJECT_FOLDER_PREFIX = "bucket_directory1/.../bucket_directoryN/my_file.ext"
      where my_file is the file's name, and ext is its extension.


      Attention: after running this function for fetching AWS Simple Storage System (S3), 
      your 'AWS Access key ID' and your 'Secret access key' will be requested.
      The 'Secret access key' will be hidden through dots, so it cannot be visualized or copied by
      other users. On the other hand, the same is not true for 'Access key ID', the bucket's name 
      and the prefix. All of these are sensitive information from the organization.
      Therefore, after importing the information, always remember of cleaning the output of this cell
      and of removing such information from the strings.
      Remember that these data may contain privilege for accessing the information, so it should not
      be used for non-authorized people.

      Also, remember of deleting the imported files from the workspace after finishing the analysis.
      The costs for storing the files in S3 is quite inferior than those for storing directly in the
      workspace. Also, files stored in S3 may be accessed for other users than those with access to
      the notebook's workspace.
    """


    if (source == 'google'):
        
        try: # try accessing the connector, if it exists
            if Connectors.google_drive_connector:
                if Connectors.persistent:
                    # Run if there is a persistent connector (if it is not None):
                    google_drive_connector = Connectors.google_drive_connector
                else: # Create the connector    
                    google_drive_connector = MountGoogleDrive()
                    Connectors.google_drive_connector = google_drive_connector

        except: # Create the connector    
            google_drive_connector = MountGoogleDrive()
            Connectors.google_drive_connector = google_drive_connector

    elif (source == 'aws'):
        InvalidInputsError("colab-utils is a simplified version of idsw that cannot access S3.")

    else:
        
        raise InvalidInputsError("Select a valid source: \'google\' for mounting Google Drive; or \'aws\' for accessing AWS S3 Bucket.")


def upload_to_or_download_file_from_colab (connect_to_gdrive = False, action = 'download', file_to_download_from_colab = None):
    """
    upload_to_or_download_file_from_colab (action = 'download', file_to_download_from_colab = None):
    
    : param: connect_to_gdrive (bool) = False not to connect to Google Drive. False to connect to it.
    
    : param: action = 'download' to download the file to the local machine
      action = 'upload' to upload a file from local machine to
      Google Colab's instant memory
    
    : param: file_to_download_from_colab = None. This parameter is obbligatory when
      action = 'download'. 
      Declare as file_to_download_from_colab the file that you want to download, with
      the correspondent extension.
      It should not be declared in quotes.
      e.g. to download a dictionary named dict, object_to_download_from_colab = 'dict.pkl'
      To download a dataframe named df, declare object_to_download_from_colab = 'df.csv'
      To export a model named keras_model, declare object_to_download_from_colab = 'keras_model.h5'
    """
    
    try: # try accessing the connector, if it exists
        if Connectors.google_drive_connector:
            if Connectors.persistent:
                # Run if there is a persistent connector  (if it is not None):
                google_drive_connector = Connectors.google_drive_connector
            else: # Create the connector    
                google_drive_connector = MountGoogleDrive()
                if (connect_to_gdrive):
                    google_drive_connector.mount_drive()

                Connectors.google_drive_connector = google_drive_connector

    except: # Create the connector    
        google_drive_connector = MountGoogleDrive()
        if (connect_to_gdrive):
                    google_drive_connector.mount_drive()

        Connectors.google_drive_connector = google_drive_connector

        
    if (action == 'upload'):
            
        google_drive_connector = google_drive_connector.upload_to_colab()
        return google_drive_connector.colab_files_dict
        
    elif (action == 'download'):
            
        google_drive_connector = google_drive_connector.download_from_colab(file_to_download_from_colab)

    else:
        raise InvalidInputsError("Please, select a valid action, \'download\' or \'upload\'.")


def load_pandas_dataframe (file_directory_path, file_name_with_extension, load_txt_file_with_json_format = False, how_missing_values_are_registered = None, has_header = True, decimal_separator = '.', txt_csv_col_sep = "comma", load_all_sheets_at_once = False, sheet_to_load = None, json_record_path = None, json_field_separator = "_", json_metadata_prefix_list = None):
    """
    load_pandas_dataframe (file_directory_path, file_name_with_extension, load_txt_file_with_json_format = False, how_missing_values_are_registered = None, has_header = True, decimal_separator = '.', txt_csv_col_sep = "comma", load_all_sheets_at_once = False, sheet_to_load = None, json_record_path = None, json_field_separator = "_", json_metadata_prefix_list = None):
    
    Pandas documentation:
     pd.read_csv: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html
     pd.read_excel: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html
     pd.json_normalize: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.json_normalize.html
     Python JSON documentation:
     https://docs.python.org/3/library/json.html
    
    ## WARNING: Use this function to load dataframes stored on Excel (xls, xlsx, xlsm, xlsb, odf, ods and odt), 
       JSON, txt, or CSV (comma separated values) files. Tables in webpages or html files can also be read.
    
    : param: file_directory_path - (string, in quotes): input the path of the directory (e.g. folder path) 
     where the file is stored. e.g. file_directory_path = "/" or file_directory_path = "/folder"
    
    : param: FILE_NAME_WITH_EXTENSION - (string, in quotes): input the name of the file with the 
      extension. e.g. FILE_NAME_WITH_EXTENSION = "file.xlsx", or, 
      FILE_NAME_WITH_EXTENSION = "file.csv", "file.txt", or "file.json"
      Again, the extensions may be: xls, xlsx, xlsm, xlsb, odf, ods, odt, json, txt or csv. Also,
      html files and webpages may be also read.
    
      You may input the path for an HTML file containing a table to be read; or 
      a string containing the address for a webpage containing the table. The address must start
      with www or htpp. If a website is input, the full address can be input as FILE_DIRECTORY_PATH
      or as FILE_NAME_WITH_EXTENSION.
    
    : param: load_txt_file_with_json_format = False. Set load_txt_file_with_json_format = True 
      if you want to read a file with txt extension containing a text formatted as JSON 
      (but not saved as JSON).
      WARNING: if load_txt_file_with_json_format = True, all the JSON file parameters of the 
      function (below) must be set. If not, an error message will be raised.
    
    : param: HOW_MISSING_VALUES_ARE_REGISTERED = None: keep it None if missing values are registered as None,
      empty or np.nan. Pandas automatically converts None to NumPy np.nan objects (floats).
      This parameter manipulates the argument na_values (default: None) from Pandas functions.
      By default the following values are interpreted as NaN: ‘’, ‘#N/A’, ‘#N/A N/A’, ‘#NA’, ‘-1.#IND’, 
     ‘-1.#QNAN’, ‘-NaN’, ‘-nan’, ‘1.#IND’, ‘1.#QNAN’, ‘<NA>’, ‘N/A’, ‘NA’, ‘NULL’, ‘NaN’, 
      ‘n/a’, ‘nan’, ‘null’.

      If a different denomination is used, indicate it as a string. e.g.
      HOW_MISSING_VALUES_ARE_REGISTERED = '.' will convert all strings '.' to missing values;
      HOW_MISSING_VALUES_ARE_REGISTERED = 0 will convert zeros to missing values.

      If dict passed, specific per-column NA values. For example, if zero is the missing value
      only in column 'numeric_col', you can specify the following dictionary:
      how_missing_values_are_registered = {'numeric-col': 0}
    
    : param: has_header = True if the the imported table has headers (row with columns names).
      Alternatively, has_header = False if the dataframe does not have header.
    
    : param: DECIMAL_SEPARATOR = '.' - String. Keep it '.' or None to use the period ('.') as
      the decimal separator. Alternatively, specify here the separator.
      e.g. DECIMAL_SEPARATOR = ',' will set the comma as the separator.
      It manipulates the argument 'decimal' from Pandas functions.
    
    : param: txt_csv_col_sep = "comma" - This parameter has effect only when the file is a 'txt'
      or 'csv'. It informs how the different columns are separated.
      Alternatively, txt_csv_col_sep = "comma", or txt_csv_col_sep = "," 
      for columns separated by comma;
      txt_csv_col_sep = "whitespace", or txt_csv_col_sep = " " 
      for columns separated by simple spaces.
      You can also set a specific separator as string. For example:
      txt_csv_col_sep = '\s+'; or txt_csv_col_sep = '\t' (in this last example, the tabulation
      is used as separator for the columns - '\t' represents the tab character).
    
    ## Parameters for loading Excel files:
    
    : param: load_all_sheets_at_once = False - This parameter has effect only when for Excel files.
      If load_all_sheets_at_once = True, the function will return a list of dictionaries, each
      dictionary containing 2 key-value pairs: the first key will be 'sheet', and its
      value will be the name (or number) of the table (sheet). The second key will be 'df',
      and its value will be the pandas dataframe object obtained from that sheet.
      This argument has preference over sheet_to_load. If it is True, all sheets will be loaded.
    
    : param: sheet_to_load - This parameter has effect only when for Excel files.
      keep sheet_to_load = None not to specify a sheet of the file, so that the first sheet
      will be loaded.
      sheet_to_load may be an integer or an string (inside quotes). sheet_to_load = 0
      loads the first sheet (sheet with index 0); sheet_to_load = 1 loads the second sheet
      of the file (index 1); sheet_to_load = "Sheet1" loads a sheet named as "Sheet1".
      Declare a number to load the sheet with that index, starting from 0; or declare a
      name to load the sheet with that name.
        
    ## Parameters for loading JSON files:
    
    : param: json_record_path (string): manipulate parameter 'record_path' from json_normalize method.
      Path in each object to list of records. If not passed, data will be assumed to 
      be an array of records. If a given field from the JSON stores a nested JSON (or a nested
      dictionary) declare it here to decompose the content of the nested data. e.g. if the field
      'books' stores a nested JSON, declare, json_record_path = 'books'
    
    : param: json_field_separator = "_" (string). Manipulates the parameter 'sep' from json_normalize method.
      Nested records will generate names separated by sep. 
      e.g., for json_field_separator = ".", {‘foo’: {‘bar’: 0}} -> foo.bar.
      Then, if a given field 'main_field' stores a nested JSON with fields 'field1', 'field2', ...
      the name of the columns of the dataframe will be formed by concatenating 'main_field', the
      separator, and the names of the nested fields: 'main_field_field1', 'main_field_field2',...
    
    : param: json_metadata_prefix_list: list of strings (in quotes). Manipulates the parameter 
      'meta' from json_normalize method. Fields to use as metadata for each record in resulting 
      table. Declare here the non-nested fields, i.e., the fields in the principal JSON. They
      will be repeated in the rows of the dataframe to give the metadata (context) of the rows.
    
      e.g. Suppose a JSON with the following structure: {'name': 'Mary', 'last': 'Shelley',
      'books': [{'title': 'Frankestein', 'year': 1818}, {'title': 'Mathilda ', 'year': 1819},{'title': 'The Last Man', 'year': 1826}]},
      Here, there are nested JSONs in the field 'books'. The fields that are not nested
      are 'name' and 'last'.
      Then, json_record_path = 'books'
      json_metadata_prefix_list = ['name', 'last']
    """
    
    import os
    import json
    from pandas import json_normalize
    
    if (file_directory_path is None):
        file_directory_path = ''
    if (file_name_with_extension is None):
        file_name_with_extension = ''
    
    # Create the complete file path:
    file_path = os.path.join(file_directory_path, file_name_with_extension)
    
    # Extract the file extension
    file_extension = os.path.splitext(file_path)[1][1:]
    # os.path.splitext(file_path) is a tuple of strings: the first is the complete file
    # root with no extension; the second is the extension starting with a point: '.txt'
    # When we set os.path.splitext(file_path)[1], we are selecting the second element of
    # the tuple. By selecting os.path.splitext(file_path)[1][1:], we are taking this string
    # from the second character (index 1), eliminating the dot: 'txt'
    
    if(file_extension not in ['xls', 'xlsx', 'xlsm', 'xlsb', 'odf',
                              'ods', 'odt', 'json', 'txt', 'csv', 'html']):
        
        # Check if it is a webpage by evaluating the 3 to 5 initial characters:
        # Notice that 'https' contains 'http'
        if ((file_path[:3] == 'www') | (file_path[:4] == '/www') | (file_path[:4] == 'http')| (file_path[:5] == '/http')):
            file_extension = 'html'

            # If the address starts with a slash (1st character), remove it:
            if (file_path[0] == '/'):
                # Pick all characters from index 1:
                file_path = file_path[1:]
    
        
    # Check if the decimal separator is None. If it is, set it as '.' (period):
    if (decimal_separator is None):
        decimal_separator = '.'
    
    if ((file_extension == 'txt') | (file_extension == 'csv')): 
        # The operator & is equivalent to 'And' (intersection).
        # The operator | is equivalent to 'Or' (union).
        # pandas.read_csv method must be used.
        if (load_txt_file_with_json_format == True):
            
            print("Reading a txt file containing JSON parsed data. A reading error will be raised if you did not set the JSON parameters.\n")
            
            with open(file_path, 'r') as opened_file:
                # 'r' stands for read mode; 'w' stands for write mode
                # read the whole file as a string named 'file_full_text'
                file_full_text = opened_file.read()
                # if we used the readlines() method, we would be reading the
                # file by line, not the whole text at once.
                # https://stackoverflow.com/questions/8369219/how-to-read-a-text-file-into-a-string-variable-and-strip-newlines?msclkid=a772c37bbfe811ec9a314e3629df4e1e
                # https://www.tutorialkart.com/python/python-read-file-as-string/#:~:text=example.py%20%E2%80%93%20Python%20Program.%20%23open%20text%20file%20in,and%20prints%20it%20to%20the%20standard%20output.%20Output.?msclkid=a7723a1abfe811ecb68bba01a2b85bd8
                
            #Now, file_full_text is a string containing the full content of the txt file.
            json_file = json.loads(file_full_text)
            # json.load() : This method is used to parse JSON from URL or file.
            # json.loads(): This method is used to parse string with JSON content.
            # e.g. .json.loads() must be used to read a string with JSON and convert it to a flat file
            # like a dataframe.
            # check: https://www.pythonpip.com/python-tutorials/how-to-load-json-file-using-python/#:~:text=The%20json.load%20%28%29%20is%20used%20to%20read%20the,and%20alter%20data%20in%20our%20application%20or%20system.
            dataset = json_normalize(json_file, record_path = json_record_path, sep = json_field_separator, meta = json_metadata_prefix_list)
        
        else:
            # Not a JSON txt
        
            if (has_header == True):

                if ((txt_csv_col_sep == "comma") | (txt_csv_col_sep == ",")):

                    dataset = pd.read_csv(file_path, na_values = how_missing_values_are_registered, verbose = True, parse_dates = True, decimal = decimal_separator)
                    # verbose = True for showing number of NA values placed in non-numeric columns.
                    #  parse_dates = True: try parsing the index; infer_datetime_format = True : If True and parse_dates is enabled, pandas will attempt to infer the format of the datetime strings in 
                    # the columns, and if it can be inferred, switch to a faster method of parsing them. In some cases this can increase the 
                    # parsing speed by 5-10x.

                elif ((txt_csv_col_sep == "whitespace") | (txt_csv_col_sep == " ")):

                    dataset = pd.read_csv(file_path, delim_whitespace = True, na_values = how_missing_values_are_registered, verbose = True, parse_dates = True, decimal = decimal_separator)
                    
                    
                else:
                    
                    try:
                        
                        # Try using the character specified as the argument txt_csv_col_sep:
                        dataset = pd.read_csv(file_path, sep = txt_csv_col_sep, na_values = how_missing_values_are_registered, verbose = True, parse_dates = True, decimal = decimal_separator)
                    
                    except:
                        # An error was raised, the separator is not valid
                        raise InvalidInputsError(f"Enter a valid column separator for the {file_extension} file, like: \'comma\' or \'whitespace\'.")


            else:
                # has_header == False

                if ((txt_csv_col_sep == "comma") | (txt_csv_col_sep == ",")):

                    dataset = pd.read_csv(file_path, header = None, na_values = how_missing_values_are_registered, verbose = True, parse_dates = True, decimal = decimal_separator)

                    
                elif ((txt_csv_col_sep == "whitespace") | (txt_csv_col_sep == " ")):

                    dataset = pd.read_csv(file_path, delim_whitespace = True, header = None, na_values = how_missing_values_are_registered, verbose = True, parse_dates = True, decimal = decimal_separator)
                    
                    
                else:
                    
                    try:
                        
                        # Try using the character specified as the argument txt_csv_col_sep:
                        dataset = pd.read_csv(file_path, sep = txt_csv_col_sep, header = None, na_values = how_missing_values_are_registered, verbose = True, parse_dates = True, decimal = decimal_separator)
                    
                    except:
                        # An error was raised, the separator is not valid
                        raise InvalidInputsError(f"Enter a valid column separator for the {file_extension} file, like: \'comma\' or \'whitespace\'.")

    elif (file_extension == 'json'):
        
        with open(file_path, 'r') as opened_file:
            
            json_file = json.load(opened_file)
            # The structure json_file = json.load(open(file_path)) relies on the GC to close the file. That's not a 
            # good idea: If someone doesn't use CPython the garbage collector might not be using refcounting (which 
            # collects unreferenced objects immediately) but e.g. collect garbage only after some time.
            # Since file handles are closed when the associated object is garbage collected or closed 
            # explicitly (.close() or .__exit__() from a context manager) the file will remain open until 
            # the GC kicks in.
            # Using 'with' ensures the file is closed as soon as the block is left - even if an exception 
            # happens inside that block, so it should always be preferred for any real application.
            # source: https://stackoverflow.com/questions/39447362/equivalent-ways-to-json-load-a-file-in-python
            
        # json.load() : This method is used to parse JSON from URL or file.
        # json.loads(): This method is used to parse string with JSON content.
        # Then, json.load for a .json file
        # and json.loads for text file containing json
        # check: https://www.pythonpip.com/python-tutorials/how-to-load-json-file-using-python/#:~:text=The%20json.load%20%28%29%20is%20used%20to%20read%20the,and%20alter%20data%20in%20our%20application%20or%20system.   
        dataset = json_normalize(json_file, record_path = json_record_path, sep = json_field_separator, meta = json_metadata_prefix_list)
    
            
    elif (file_extension == 'html'):    
        
        if (has_header == True):
            
            dataset = pd.read_html(file_path, na_values = how_missing_values_are_registered, parse_dates = True, decimal = decimal_separator)
            
        else:
            
            dataset = pd.read_html(file_path, header = None, na_values = how_missing_values_are_registered, parse_dates = True, decimal = decimal_separator)
        
        
    else:
        # If it is not neither a csv nor a txt file, let's assume it is one of different
        # possible Excel files.
        print("Excel file inferred. If an error message is shown, check if a valid file extension was used: \'xlsx\', \'xls\', etc.\n")
        # For Excel type files, Pandas automatically detects the decimal separator and requires only the parameter parse_dates.
        # Firstly, the argument infer_datetime_format was present on read_excel function, but was removed.
        # From version 1.4 (beta, in 10 May 2022), it will be possible to pass the parameter 'decimal' to
        # read_excel function for detecting decimal cases in strings. For numeric variables, it is not needed, though
        
        if (load_all_sheets_at_once == True):
            
            # Corresponds to setting sheet_name = None
            
            if (has_header == True):
                
                xlsx_doc = pd.read_excel(file_path, sheet_name = None, na_values = how_missing_values_are_registered, verbose = True, parse_dates = True)
                # verbose = True for showing number of NA values placed in non-numeric columns.
                #  parse_dates = True: try parsing the index; infer_datetime_format = True : If True and parse_dates is enabled, pandas will attempt to infer the format of the datetime strings in 
                # the columns, and if it can be inferred, switch to a faster method of parsing them. In some cases this can increase the 
                # parsing speed by 5-10x.
                
            else:
                #No header
                xlsx_doc = pd.read_excel(file_path, sheet_name = None, header = None, na_values = how_missing_values_are_registered, verbose = True, parse_dates = True)
            
            # xlsx_doc is a dictionary containing the sheet names as keys, and dataframes as items.
            # Let's convert it to the desired format.
            # Dictionary dict, dict.keys() is the array of keys; dict.values() is an array of the values;
            # and dict.items() is an array of tuples with format ('key', value)
            
            # Create a list of returned datasets:
            list_of_datasets = []
            
            # Let's iterate through the array of tuples. The first element returned is the key, and the
            # second is the value
            for sheet_name, dataframe in (xlsx_doc.items()):
                # sheet_name = key; dataframe = value
                # Define the dictionary with the standard format:
                df_dict = {'sheet': sheet_name,
                            'df': dataframe}
                
                # Add the dictionary to the list:
                list_of_datasets.append(df_dict)
            
            if ControlVars.show_results: 
                print("\n")
                print(f"A total of {len(list_of_datasets)} dataframes were retrieved from the Excel file.\n")
                print(f"The dataframes correspond to the following Excel sheets: {list(xlsx_doc.keys())}\n")
                print("Returning a list of dictionaries. Each dictionary contains the key \'sheet\', with the original sheet name; and the key \'df\', with the Pandas dataframe object obtained.\n")
                print(f"Check the 10 first rows of the dataframe obtained from the first sheet, named {list_of_datasets[0]['sheet']}:\n")
                
                try:
                    # only works in Jupyter Notebook:
                    from IPython.display import display
                    display((list_of_datasets[0]['df']).head(10))
                
                except: # regular mode
                    print((list_of_datasets[0]['df']).head(10))
                
            return list_of_datasets
            
        elif (sheet_to_load is not None):        
        #Case where the user specifies which sheet of the Excel file should be loaded.
            
            if (has_header == True):
                
                dataset = pd.read_excel(file_path, sheet_name = sheet_to_load, na_values = how_missing_values_are_registered, verbose = True, parse_dates = True)
                # verbose = True for showing number of NA values placed in non-numeric columns.
                #  parse_dates = True: try parsing the index; infer_datetime_format = True : If True and parse_dates is enabled, pandas will attempt to infer the format of the datetime strings in 
                # the columns, and if it can be inferred, switch to a faster method of parsing them. In some cases this can increase the 
                # parsing speed by 5-10x.
                
            else:
                #No header
                dataset = pd.read_excel(file_path, sheet_name = sheet_to_load, header = None, na_values = how_missing_values_are_registered, verbose = True, parse_dates = True)
                
        
        else:
            #No sheet specified
            if (has_header == True):
                
                dataset = pd.read_excel(file_path, na_values = how_missing_values_are_registered, verbose = True, parse_dates = True)
                
            else:
                #No header
                dataset = pd.read_excel(file_path, header = None, na_values = how_missing_values_are_registered, verbose = True, parse_dates = True)

    if ControlVars.show_results:       
        print(f"Dataset extracted from {file_path}. Check the 10 first rows of this dataframe:\n")
        
        try:
            # only works in Jupyter Notebook:
            from IPython.display import display
            display(dataset.head(10))
                
        except: # regular mode
            print(dataset.head(10))
    
    return dataset


def json_obj_to_pandas_dataframe (json_obj_to_convert, json_obj_type = 'list', json_record_path = None, json_field_separator = "_", json_metadata_prefix_list = None):
    """
    json_obj_to_pandas_dataframe (json_obj_to_convert, json_obj_type = 'list', json_record_path = None, json_field_separator = "_", json_metadata_prefix_list = None):
    
     JSON object in terms of Python structure: list of dictionaries, where each value of a
     dictionary may be a dictionary or a list of dictionaries (nested structures).
     example of highly nested structure saved as a list 'json_formatted_list'. Note that the same
     structure could be declared and stored into a string variable. For instance, if you have a txt
     file containing JSON, you could read the txt and save its content as a string.
     json_formatted_list = [{'field1': val1, 'field2': {'dict_val': dict_val}, 'field3': [{
     'nest1': nest_val1}, {'nest2': nestval2}]}, {'field1': val1, 'field2': {'dict_val': dict_val}, 
     'field3': [{'nest1': nest_val1}, {'nest2': nestval2}]}]    

    : param: json_obj_type = 'list', in case the object was saved as a list of dictionaries (JSON format)
      json_obj_type = 'string', in case it was saved as a string (text) containing JSON.

    : param: json_obj_to_convert: object containing JSON, or string with JSON content to parse.
      Objects may be: string with JSON formatted text;
      list with nested dictionaries (JSON formatted);
      dictionaries, possibly with nested dictionaries (JSON formatted).
    
      https://docs.python.org/3/library/json.html
      https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.json_normalize.html#pandas.json_normalize
    
    : param: json_record_path (string): manipulate parameter 'record_path' from json_normalize method.
      Path in each object to list of records. If not passed, data will be assumed to 
      be an array of records. If a given field from the JSON stores a nested JSON (or a nested
      dictionary) declare it here to decompose the content of the nested data. e.g. if the field
      'books' stores a nested JSON, declare, json_record_path = 'books'
    
    : param: json_field_separator = "_" (string). Manipulates the parameter 'sep' from json_normalize method.
      Nested records will generate names separated by sep. 
      e.g., for json_field_separator = ".", {‘foo’: {‘bar’: 0}} -> foo.bar.
      Then, if a given field 'main_field' stores a nested JSON with fields 'field1', 'field2', ...
      the name of the columns of the dataframe will be formed by concatenating 'main_field', the
      separator, and the names of the nested fields: 'main_field_field1', 'main_field_field2',...
    
    : param: json_metadata_prefix_list: list of strings (in quotes). Manipulates the parameter 
      'meta' from json_normalize method. Fields to use as metadata for each record in resulting 
      table. Declare here the non-nested fields, i.e., the fields in the principal JSON. They
      will be repeated in the rows of the dataframe to give the metadata (context) of the rows.
    
      e.g. Suppose a JSON with the following structure: {'name': 'Mary', 'last': 'Shelley',
      'books': [{'title': 'Frankestein', 'year': 1818}, {'title': 'Mathilda ', 'year': 1819},{'title': 'The Last Man', 'year': 1826}]},
      Here, there are nested JSONs in the field 'books'. The fields that are not nested
      are 'name' and 'last'.
      Then, json_record_path = 'books'
      json_metadata_prefix_list = ['name', 'last']
    """

    import json
    from pandas import json_normalize
    

    if (json_obj_type == 'string'):
        # Use the json.loads method to convert the string to json
        json_file = json.loads(json_obj_to_convert)
        # json.load() : This method is used to parse JSON from URL or file.
        # json.loads(): This method is used to parse string with JSON content.
        # e.g. .json.loads() must be used to read a string with JSON and convert it to a flat file
        # like a dataframe.
        # check: https://www.pythonpip.com/python-tutorials/how-to-load-json-file-using-python/#:~:text=The%20json.load%20%28%29%20is%20used%20to%20read%20the,and%20alter%20data%20in%20our%20application%20or%20system.
    
    elif (json_obj_type == 'list'):
        
        # make the json_file the object itself:
        json_file = json_obj_to_convert
    
    else:
        raise InvalidInputsError ("Enter a valid JSON object type: \'list\', in case the JSON object is a list of dictionaries in JSON format; or \'string\', if the JSON is stored as a text (string variable).")
    
    dataset = json_normalize(json_file, record_path = json_record_path, sep = json_field_separator, meta = json_metadata_prefix_list)
    
    if ControlVars.show_results: 
        print(f"JSON object converted to a flat dataframe object. Check the 10 first rows of this dataframe:\n")
        
        try:
            # only works in Jupyter Notebook:
            from IPython.display import display
            display(dataset.head(10))
                
        except: # regular mode
            print(dataset.head(10))
    
    return dataset


def convert_variable_or_iterable_to_single_column_df (iterable, column_label = None, column_type = None):
    """
    convert_variable_or_iterable_to_single_column_df (iterable, column_label = None, column_type = None)

    Use this function to convert an iterable (array, list, tuple, etc) into a single-column
    Pandas dataframe, so that you may directly apply each one of the ETL functions below to this iterable, 
    with no modifications.
    Notice that the input of a string will result in a dataframe where each row contains a character.
    
    : param: iterable: object to be converted (list, tuple, array, etc).
      Input an object here.
    : param: column_lable = string with the name that the column will receive.
      Example: column_label = 'column1' will create a dataframe with a column named as 'column1'

    : param: column_type = None
      Set a specific type for the column: int, str, float, np.datetime64, 'datetime64[ns]', etc.
      Examples: column_type = str; column_type = np.float64; column_type = np.datetime64; 
      column_type = int, column_type = 'datetime64[ns]'
      When the parameter is passed, the column will be set as it. If not, the standard read format
      will be used.
    """

    if (column_label is None):
        column_label = 'column1'
    
    single_column_df = pd.DataFrame(data = {

        column_label: np.array(iterable)
    })

    if (column_type is not None):
        single_column_df[column_label] = single_column_df[column_label].astype(column_type)

    if ControlVars.show_results: 
        print(f"Iterable with original type {type(iterable)} converted into a Pandas dataframe containing a single column named as '{column_label}'.")
        print("Check the 10 first rows of this returned dataframe:\n")
            
        try:
            # only works in Jupyter Notebook:
            from IPython.display import display
            display(single_column_df.head(10))
                    
        except: # regular mode
            print(single_column_df.head(10))
        
    return single_column_df


def set_schema_pd_df (df, schema_list = [{'column_name': None, 'column_type': None}]):
    """
    set_schema_pd_df (df, schema_list = [{'column_name': None, 'column_type': None}]):

    USE THIS FUNCTION TO SET THE SCHEMA (COLUMN TYPES) OF A PANDAS DATAFRAME.
    You may set only some of the columns; a single column; or no column, keeping others
    as default.

    : param: schema_list: list of dictionaries containing the columns' names and the types they
      must have. Add a new dictionary for each column to have its type modified, but
      keep always the same keys. If one or two keys are None or with an invalid type,
      the column will be ignored., Add as much dictionaries as you want as elements from
      the list.
      Examples of column types: column_type: str; column_type: np.float64; 
      column_type: np.datetime64; column_type: int
      Examples of schema_list:
      schema_list = [{'column_name': 'column1', 'column_type': str}] will only set 'column1' as string.
      schema_list = [{'column_name': 'column1', 'column_type': 'datetime64[ns]'},
      {'column_name': 'column2', 'column_type': str},
      {'column_name': 'column3', 'column_type': float},] will set 'column1' as a datetime64, 'column2'
      as string (text), and 'column3' as float (numeric).
      schema_list = [{'column_name': 'name', 'column_type': str},
      {'column_name': 'money', 'column_type': float},] will set column 'name' 
      as string (text), and column 'money' as float (numeric).
    """
    
    dataset = df.copy(deep = True)

    for schema in schema_list:
        try:
            column_name, column_type = schema['column_name'], schema['column_type']
            if ((column_name is not None) & (column_type is not None)):
                try:
                    dataset[column_name] = np.array(dataset[column_name], dtype = column_type)
                except:
                    pass

        except:
            pass
    
    print("Check the 10 first rows of the returned dataframe:\n")
        
    try:
        # only works in Jupyter Notebook:
        from IPython.display import display
        display(dataset.head(10))
                
    except: # regular mode
        print(dataset.head(10))
    
    print("\n")
    df_dtypes = dataset.dtypes
    # Now, the df_dtypes series has the original columns set as index, but this index has no name.
    # Let's rename it using the .rename method from Pandas Index object:
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Index.rename.html#pandas.Index.rename
    # To access the Index object, we call the index attribute from Pandas dataframe.
    # By setting inplace = True, we modify the object inplace, by simply calling the method:
    df_dtypes.index.rename(name = 'dataframe_column', inplace = True)
    # Let's also modify the series label or name:
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.rename.html
    df_dtypes.rename('dtype_series', inplace = True)
    
    if ControlVars.show_results: 
        print("Dataframe\'s variables types in accordance with the provided schema:\n")
        try:
            display(df_dtypes)
        except:
            print(df_dtypes)
            
    return dataset


def export_pd_dataframe_as_csv (dataframe_obj_to_be_exported, new_file_name_without_extension, file_directory_path = None):
    """
    export_pd_dataframe_as_csv (dataframe_obj_to_be_exported, new_file_name_without_extension, file_directory_path = None)

    WARNING: all files exported from this function are .csv (comma separated values)
    
    : param: dataframe_obj_to_be_exported: dataframe object that is going to be exported from the
      function. Since it is an object (not a string), it should not be declared in quotes.
      example: dataframe_obj_to_be_exported = dataset will export the dataset object.
      ATTENTION: The dataframe object must be a Pandas dataframe.
    
    : param: FILE_DIRECTORY_PATH - (string, in quotes): input the path of the directory 
      (e.g. folder path) where the file is stored. e.g. FILE_DIRECTORY_PATH = "/" 
      or FILE_DIRECTORY_PATH = "/folder"
      If you want to export the file to AWS S3, this parameter will have no effect.
      In this case, you can set FILE_DIRECTORY_PATH = None

    : param: new_file_name_without_extension - (string, in quotes): input the name of the 
      file without the extension. e.g. new_file_name_without_extension = "my_file" 
      will export a file 'my_file.csv' to notebook's workspace.
    """
    import os

    
    # Create the complete file path:
    file_path = os.path.join(file_directory_path, new_file_name_without_extension)
    # Concatenate the extension ".csv":
    file_path = file_path + ".csv"

    dataframe_obj_to_be_exported.to_csv(file_path, index = False)

    if ControlVars.show_results: 
        print(f"Dataframe {new_file_name_without_extension} exported as CSV file to notebook\'s workspace as \'{file_path}\'.")
        print("Warning: if there was a file in this file path, it was replaced by the exported dataframe.")


def export_pd_dataframe_as_excel (file_name_without_extension, exported_tables = [{'dataframedataframe_obj_to_be_exported': None, 'excel_sheet_name': None}], file_directory_path = None):
    """
    export_pd_dataframe_as_excel (file_name_without_extension, exported_tables = [{'dataframedataframe_obj_to_be_exported': dataframe_obj_to_be_exported, 'excel_sheet_name': excel_sheet_name}], file_directory_path = None):
    
    This function allows the user to export several dataframes as different sheets from a single
    Excel file.
    WARNING: all files exported from this function are .xlsx

    : param: file_name_without_extension - (string, in quotes): input the name of the 
      file without the extension. e.g. new_file_name_without_extension = "my_file" 
      will export a file 'my_file.xlsx' to notebook's workspace.

    : param: exported_tables is a list of dictionaries.
      User may declare several dictionaries, as long as the keys are always the same, and if the
      values stored in keys are not None.
      
      : key 'dataframe_obj_to_be_exported': dataframe object that is going to be exported from the
      function. Since it is an object (not a string), it should not be declared in quotes.
      example: dataframe_obj_to_be_exported = dataset will export the dataset object.
      ATTENTION: The dataframe object must be a Pandas dataframe.

      : key 'excel_sheet_name': string containing the name of the sheet to be written on the
      exported Excel file. Example: excel_sheet_name = 'tab_1' will save the dataframe in the
      sheet 'tab_1' from the file named as file_name_without_extension.

      examples: exported_tables = [{'dataframe_obj_to_be_exported': dataset1, 'excel_sheet_name': 'sheet1'},]
      will export only dataset1 as 'sheet1';
      exported_tables = [{'dataframe_obj_to_be_exported': dataset1, 'excel_sheet_name': 'sheet1'},
      {'dataframe_obj_to_be_exported': dataset2, 'excel_sheet_name': 'sheet2']
      will export dataset1 as 'sheet1' and dataset2 as 'sheet2'.

      Notice that if the file does not contain the exported sheets, they will be created. If it has,
      the sheets will be replaced.
    
    : param: FILE_DIRECTORY_PATH - (string, in quotes): input the path of the directory 
      (e.g. folder path) where the file is stored. e.g. FILE_DIRECTORY_PATH = "/" 
      or FILE_DIRECTORY_PATH = "/folder"
      If you want to export the file to AWS S3, this parameter will have no effect.
      In this case, you can set FILE_DIRECTORY_PATH = None
    """

    import os

    # Create the complete file path:
    file_path = os.path.join(file_directory_path, file_name_without_extension)
    # Concatenate the extension ".csv":
    file_path = file_path + ".xlsx"

    # Pandas ExcelWriter class:
    # https://pandas.pydata.org/docs/reference/api/pandas.ExcelWriter.html#pandas.ExcelWriter
    # Pandas to_excel method:
    # https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_excel.html

    try:
        # The replacement of a Sheet will only occur in the append ('a') mode.
        # 'a' is a mode available for the cases where an Excel file is already present.
        # Let's check if there is an Excel file previously created, so that we will not
        # delete it:
        with pd.ExcelWriter(file_path, date_format = "YYYY-MM-DD",
                            datetime_format = "YYYY-MM-DD HH:MM:SS",
                            mode = 'a', if_sheet_exists = 'replace') as writer:
            
            for storage_dict in exported_tables:
                df, sheet = storage_dict['dataframe_obj_to_be_exported'], storage_dict['excel_sheet_name']
                
                if ((df is not None) & (sheet is not None) & (type(df) == pd.DataFrame)):
                    # Guarantee sheet name is a string
                    sheet = str(sheet)
                    df.to_excel(writer, sheet_name = sheet, na_rep='', 
                                header = True, index = False, 
                                startrow = 0, startcol = 0, merge_cells = False, 
                                inf_rep = 'inf')

    except:
        # The context manager created by class ExcelWriter with 'a' mode returns an error when
        # there is no Excel file available. Since we do not have the risk of overwriting the file,
        # we can open the writer in write ('w') mode to create a new spreadsheet:
        with pd.ExcelWriter(file_path, date_format = "YYYY-MM-DD",
                            datetime_format = "YYYY-MM-DD HH:MM:SS", mode = 'w') as writer:
            
            for storage_dict in exported_tables:
                df, sheet = storage_dict['dataframe_obj_to_be_exported'], storage_dict['excel_sheet_name']
                
                if ((df is not None) & (sheet is not None) & (type(df) == pd.DataFrame)):
                    # Guarantee sheet name is a string
                    sheet = str(sheet)
                    df.to_excel(writer, sheet_name = sheet, index = False, 
                                startrow = 0, startcol = 0, merge_cells = False, 
                                inf_rep = 'inf')

    if ControlVars.show_results: 
        print(f"Dataframes exported as Excel file to notebook\'s workspace as \'{file_path}\'.")
        print("Warning: if there was a sheet with the same name as the exported ones, it was replaced by the exported dataframe.")


def manipulate_sqlite_db (file_path, table_name, action = 'fetch_table', pre_created_engine = None, df = None):
    """
    manipulate_sqlite_db (file_path, table_name, action = 'fetch_table', pre_created_engine = None, df = None)

    : param: file_path: full path of the SQLite file. It may start with './' or '/', but with no more than 2 slashes.
      It is a string: input in quotes. Example: file_path = '/my_db.db'
    : param: table_name: string with the name of the table that will be fetched or updated.
      Example: table_name = 'main_table'

    : param: action = 'fetch_table' to access a table named table_name from the database.
      action = 'update_table' to update a table named table_name from the database.

    : param: pre_created_engine = None - If None, a new engine will be created. If an engine was already created, pass it as argument:
      pre_created_engine = engine

    : param: df = None - if a table is going to be updated, input here the new Pandas dataframe (object) correspondent to the table.
      Example: df = dataset.
    """

    
    try: # try accessing the connector, if it exists
        if (Connectors.sqlite_connector):
            if Connectors.persistent:
                # Run if there is a persistent connector  (if it is not None):
                sqlite_connector = Connectors.sqlite_connector
            else:
                # Create the connector
                sqlite_connector = SQLiteConnection(file_path, pre_created_engine)
    
    except:
        # Create the connector
        sqlite_connector = SQLiteConnection(file_path, pre_created_engine)


    if (action == 'fetch_table'):

            df, engine = sqlite_connector.fetch_table(table_name)
            Connectors.sqlite_connector = sqlite_connector
            
            return df, engine
        
    elif (action == 'update_table'):

            df, engine = sqlite_connector.update_or_create_table(table_name)
            Connectors.sqlite_connector = sqlite_connector

            return df, engine


def bigquery_pipeline(project = '', dataset = '', already_authenticated = True,
                        authentication_method = 'manual',
                        vault_secret_path = '', app_role = '', app_secret = '',
                        action = 'connect',
                        query = '',
                        show_table = True, export_csv = False, saving_directory_path = "",
                        table = '',
                        df = None,
                        column = '', values_to_delete = None,
                        old_value = None,
                        updated_value = None, comparative_column = None, value_to_search = None, 
                        string_column = '', str_or_substring_to_search = '',
                        view_id = ''
                        ):
    """
    Pipeline for fetching or updating data stored on Google Cloud Platform (GCP).


    : param: action (str): which action will be performed.
        
        - 'connect': only a connector will be created in the memory. Requires no query declaration
        
        - 'run_sql_query': run a specified SQL query (parameter 'query') and returns a pandas dataframe.
            Notice that 'query' cannot be an empty string for using this action.
        
        - 'get_full_table': run a query to return the full content from a table declared in parameter 'table'
            Notice that 'table' cannot be an empty string or None object for using this action.
        
        - 'write_data_on_bigquery_table': write data from a dataframe declared in
            parameter 'df' on a table declared in parameter 'table'. Notice that neither 'table' nor 'df' cannot
            be empty strings or None objects for using this action.
        
        - 'delete_specific_values_from_column_on_table': look at a column specified in the parameter 'column' from a
            table declared in parameter 'table'; and then search for values specified as 'values_to_delete'. 
            Notice that 'column', 'table' or 'values_to_delete' cannot be empty strings or None objects for 
            using this action.
        
        - 'update_specific_value_from_column_on_table': look at a column specified in the parameter 'column' from a
            table declared in parameter 'table'; and then search for a value specified as 'old_value' in this column. 
            If this value is found on, then update the row  with the value specified in 'updated_value'. 
            Notice that 'column', 'table', 'old_value', or 'updated_value' cannot be empty strings or 
            None objects for using this action.

        - 'update_entire_column_from_table': look at a column specified in the parameter 'column' from a
            table declared in parameter 'table'; and then replace all values from this column with the value defined
            as 'updated_value'. Notice that 'column', 'table' or 'updated_value' cannot be empty strings or None objects 
            for using this action.

        - 'update_value_when_finding_str_or_substring_on_another_column': look at a column specified in the parameter 
            'column' from a table declared in parameter 'table'; and then search for a string or substring 
            specified as 'str_or_substring_to_search' in a second column named 'string_column'. 
            If this value is found on 'string_column', then update the
            correspondent row in 'column' with the value specified in 'updated_value'. 
            Notice that 'column', 'table', 'str_or_substring_to_search' or 'string_column' cannot be empty 
            strings or None objects for using this action.

        - 'update_value_when_finding_numeric_value_on_another_column': look at a column specified in the parameter 
            'column' from a table declared in parameter 'table'; and then search for a numeric value
            specified as 'value_to_search' in a second column named 'comparative_column'. 
            If this value is found on 'comparative_column', then update the
            correspondent row in 'column' with the value specified in 'updated_value'. 
            Notice that 'column', 'table', 'value_to_search', 'updated_value' or 'comparative_column' cannot be empty 
            strings or None objects for using this action.
        
        - 'create_new_view': given an ID defined as 'view_id', create a new view with this ID if it does not exists.
            The view is created following the instructions passed as 'query'.
            A view is a dynamic query that is automatically updated when data is modified or added. In SAP system,
            the views are called transactions. Notice that 'view_id' or 'query' cannot be None or empty for 
            using this action.

    : param: project (str): project name on BigQuery
    : param: dataset (str): dataset that user wants to connect

    
    : param: already_authenticated (bool): True if the manual connection to GCP was already performed
        and so the user is authorized.
        
        This connection can be done by running the following command directly on a cell or on the console
        from Python environment. Attention: follow the command line authentication instruction below.

            !gcloud auth application-default login
        
    
        COMMAND LINE AUTHENTICATION INSTRUCTION
            
            - BEFORE INSTATIATING THE CLASS, FOLLOW THESE GUIDELINES!
            
            1. Copy and run the following line in a notebook cell. Do not add quotes.
            The "!" sign must be added to indicate the use of a command line software:

                !gcloud auth application-default login
            
            2. Also, you can run the SQL query on GCP console and, when the query results appear, click on EXPLORE DATA - Explore with Python notebook.
            It will launch a Google Colab Python notebook that you may run for data exploring, manipulation and exporting.

            2.1. To export data, you expand the hamburger icon on the left side of the Google Colab, click on Files, and then select an exported CSV or other files. Finally, click on the ellipsis (3 dots) and select Download to obtain it.
            

        Install Google Cloud Software Development Kit (SDK) before running
                
        General Instructions for Installation: https://cloud.google.com/sdk/docs/install-sdk?hl=pt-br#installing_the_latest_version
        Instructions for Windows: https://cloud.google.com/sdk/docs/install-sdk?hl=pt-br#windows
        Instructions for Mac OS: https://cloud.google.com/sdk/docs/install-sdk?hl=pt-br#mac
        Instructions for Linux: https://cloud.google.com/sdk/docs/install-sdk?hl=pt-br#linux
        
        From: https://stackoverflow.com/questions/39419754/downloading-and-importing-google-cloud-python
        First, make sure you have installed gcloud on your system then run the commands like this:

        First: gcloud components update in your terminal.
        then: pip install google-cloud
        And for the import error:
        Adding "--ignore-installed" to pip command may work.
        This might be a bug in pip - see this page for more details: https://github.com/pypa/pip/issues/2751

        This pipeline may be blocked also due to security configurations, and may fail on some Virtual Private Networks (VPNs).
        - Try to run this pipeline outside of the VPN in case it fails
    

    : param: authentication_method (str): 'manual' or 'vault' authentication
            : param: authetication_method = 'manual' for GCP standard manual authentication on browser. System will try
                to access the authorization window, in case it was not done yet.
            : param: authetication_method = 'vault' for Vault automatic authentication, dependent on corporate
                cibersecurity and data asset.

    : param: vault_secret_path (str): path to access the secret
    : params: vault_secret_path = '', app_role = '', app_secret = '' are the parameters for vault authorization
    

    : param: show_table (bool): keep as True to print the queried table, set False to hide it.
    : param: export_csv (bool): set True to export the queried table as CSV file, or set False not to export it.
    : param: saving_directory_path (str): full path containing directories and table name, 
        with .csv extension, used when export_csv = True

    : param: table (str): name of the table to be retrieved. Full table name is `{self.project}.{self.dataset}.{str(table)}`
    : param: df (pd.DataFrame): Pandas dataframe to be written on BigQuery table
    
    : param: column (str): is the column name on a given BigQuery table (a string).
    : param: values_to_delete is a single value (numeric or string) or an iterable containing a set
        of values to be deleted.
    
    : param: old_value: value that must be replaced
    : param: updated_value: new value to be added.
    
    : param: comparative_column (str): column containing a numeric value that will be searched.
    : param: value_to_search: numeric value that will be searched 
        on column 'comparative_colum'. When it is find, the value on 'column' will be updated.
    
    : param: string_column (str): column containing a string or substring that will be searched.
    : param: str_or_substring_to_search (str): (in quotes): string or substring that will be searched on 
        column 'string_column'. When it is find, the value on 'column' will be updated.

    : param: view_id (str): The ID of the view to be created. If no ID is provided, a table is created
    """
    
    try: # try accessing the connector, if it exists
        if (Connectors.gcp_connector):
            if Connectors.persistent:
                # Run if there is a persistent connector  (if it is not None):
                gcp_connector = Connectors.gcp_connector
            else:
                # Create the connector
                gcp_connector = GCPBigQueryConnection(project, dataset, already_authenticated)
                gcp_connector = gcp_connector.authenticate(authentication_method, vault_secret_path, app_role, app_secret)
        
    except:
        # Create the connector
        gcp_connector = GCPBigQueryConnection(project, dataset, already_authenticated)
        gcp_connector = gcp_connector.authenticate(authentication_method, vault_secret_path, app_role, app_secret)
    

    if (action == 'connect'):
        Connectors.gcp_connector = gcp_connector
        return gcp_connector
    
    elif (action == 'run_sql_query'):
        df = gcp_connector.run_sql_query(query, show_table, export_csv, saving_directory_path)
        Connectors.gcp_connector = gcp_connector
        return df
    
    elif (action == 'get_full_table'):
        df_table = gcp_connector.get_full_table(table, show_table, export_csv, saving_directory_path)
        Connectors.gcp_connector = gcp_connector
        return df_table
    
    elif (action == 'write_data_on_bigquery_table'):
        gcp_connector = gcp_connector.write_data_on_bigquery_table(table, df)
        Connectors.gcp_connector = gcp_connector
        return gcp_connector
    
    elif (action == 'delete_specific_values_from_column_on_table'):
        df_table = gcp_connector.delete_specific_values_from_column_on_table(table, column, values_to_delete, show_table, export_csv, saving_directory_path)
        Connectors.gcp_connector = gcp_connector
        return df_table
    
    elif (action == 'update_specific_value_from_column_on_table'):
        df_table = gcp_connector.update_specific_value_from_column_on_table(table, column, old_value, updated_value, show_table, export_csv, saving_directory_path)
        Connectors.gcp_connector = gcp_connector
        return df_table
    
    elif (action == 'update_entire_column_from_table'):
        df_table = gcp_connector.update_entire_column_from_table(table, column, updated_value, show_table, export_csv, saving_directory_path)
        Connectors.gcp_connector = gcp_connector
        return df_table
    
    elif (action == 'update_value_when_finding_str_or_substring_on_another_column'):
        df_table = gcp_connector.update_value_when_finding_str_or_substring_on_another_column(table, column, updated_value, string_column, str_or_substring_to_search, show_table, export_csv, saving_directory_path)
        Connectors.gcp_connector = gcp_connector
        return df_table
    
    elif (action == 'update_value_when_finding_numeric_value_on_another_column'):
        df_table = gcp_connector.update_value_when_finding_numeric_value_on_another_column(table, column, updated_value, comparative_column, value_to_search, show_table, export_csv, saving_directory_path)
        Connectors.gcp_connector = gcp_connector
        return df_table
    
    elif (action == 'create_new_view'):
        df = gcp_connector.create_new_view(view_id, query, show_table, export_csv, saving_directory_path)
        return df
    

def sqlserver_pipeline (server, 
                  database,
                  username = '', 
                  password = '',
                  system = 'windows',
                  show_schema = True, export_csv = False, saving_directory_path = "",
                  query = '', show_table = True,
                  table = '',
                  tag = '', variable_name = None,   
                  ):
    """
    Pipeline for fetching or updating data stored on Microsoft SQL Server.

    : param: system = 'windows', 'macos' or 'linux'

        If the user passes the argument, use them. Otherwise, use the standard values.
        Set the class objects' attributes.
        Suppose the object is named assistant. We can access the attribute as:
        assistant.assistant_startup, for instance.
        So, we can save the variables as objects' attributes.

        INSTALL ODBC DRIVER IF USING MACOS OR LINUX (Windows already has it):
        - MacOS Installation: https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos?view=sql-server-ver16
        - Linux Installation: https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver16&tabs=alpine18-install%2Calpine17-install%2Cdebian8-install%2Credhat7-13-install%2Crhel7-offline
        

    : param: action (str): which action will be performed.
        
        - 'connect': only a connector will be created in the memory. Requires no query declaration
        
        - 'get_db_schema': get the schema of tables (data model) stored on SQL Server.

        - 'run_sql_query': run a specified SQL query (parameter 'query') and returns a pandas dataframe.
            Notice that 'query' cannot be an empty string for using this action.

        - 'get_full_table': run a query to return the full content from a table declared in parameter 'table'
            Notice that 'table' cannot be an empty string or None object for using this action.

        - 'query_specific_tag_ip21sqlserver': if the IP21 plant information system (PIMS) is stored on SQL Server,
            use this action to query only a specific tag (variable or attribute). Notice that the parameter 'tag'
            cannot be empty string or None object for using this action. The parameter 'variable_name' is optional
            and can be used to modify the tag to a name of variable easier to understand.
    
    : param: show_schema (bool): if True, the schema of the tables on the SQL Server will be shown.
    : param: show_table (bool): keep as True to print the queried table, set False to hide it.
    : param: export_csv (bool): set True to export the queried table as CSV file, or set False not to export it.
    : param: saving_directory_path (str): full path containing directories and table name, 
        with .csv extension, used when export_csv = True

    : param: table (str): string containing the name of the table that will be queried.

    : param: tag (str): string with tag as registered in IP21. e.g. tag = 'ABC00AA101-01'.
    : param: variable_name (str): string containing a more readable name for the tag, that will be also shown.
        e.g. variable_name = 'Temperature in C'
    
    """
    

    try: # try accessing the connector, if it exists
        if (Connectors.sqlserver_connector):
            if Connectors.persistent:
                # Run if there is a persistent connector  (if it is not None):
                sqlserver_connector = Connectors.sqlserver_connector
            else:
                # Create the connector
                sqlserver_connector = SQLServerConnection(server, database, username, password, system)
            
    except:
        # Create the connector
        sqlserver_connector = SQLServerConnection(server, database, username, password, system)
    

    if (action == 'connect'):
        Connectors.sqlserver_connector = sqlserver_connector
        return sqlserver_connector
    
    elif (action == 'get_db_schema'):
        sqlserver_connector = sqlserver_connector.get_db_schema(show_schema, export_csv, saving_directory_path)
        Connectors.sqlserver_connector = sqlserver_connector
        return sqlserver_connector
    
    elif (action == 'run_sql_query'):
        sqlserver_connector = sqlserver_connector.run_sql_query(query, show_table, export_csv, saving_directory_path)
        Connectors.sqlserver_connector = sqlserver_connector
        return sqlserver_connector
    
    elif (action == 'get_full_table'):
        sqlserver_connector = sqlserver_connector.get_full_table(table, show_table, export_csv, saving_directory_path)
        Connectors.sqlserver_connector = sqlserver_connector
        return sqlserver_connector
    
    elif (action == 'query_specific_tag_ip21sqlserver'):
        sqlserver_connector = sqlserver_connector.query_specific_tag_ip21sqlserver(tag, variable_name, show_table, export_csv, saving_directory_path)
        Connectors.sqlserver_connector = sqlserver_connector
        return sqlserver_connector
    
