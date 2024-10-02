import os

# This function sets up the configuration for the script, specifying where the CSV files are stored and what file extension the script should look for.
def get_config():
    """Get the configuration settings.

    Returns
    -------
    dict
        Configuration dictionary.
    """
    # Find the absolute path of the directory where the script is located, and joins the current directory (s3 upload) to the subfolder (csv_files)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    csv_dir = os.path.join(current_dir, 'csv_files')
    
    # Create our config dictionary. It will hold our file path to the csv_dir, and also the expected extension of the files that we want to process.
    config = {
        'csv_dir': csv_dir,
        'expected_file_extension': '.csv'
    }
    
    return config
   
def check_directory_exists(dir):
    """Check if a directory exists.

    Parameters
    ----------
    dir : str
        Directory path.

    Returns
    -------
    bool
        True if the directory exists, False otherwise.
    """
    if not os.path.exists(dir):
        return False
    
    return True
        
        
    
def list_files(dir):
    """List files in the specified directory.

    Parameters
    ----------
    dir : str
        Directory path.

    Returns
    -------
    list of str
        List of filenames in the directory.

    Raises
    ------
    OSError
        If the directory cannot be accessed.
    """
    # Try to assign the files in the directory to a list, and return that list of files.
    try:
        return os.listdir(dir)
    except OSError as e:
        print(f"\nError accessing {dir}.\nDetails: {e}")
        raise
        
    
        
def validate_file_type(filename, expected_extension):
    """Validate the file type based on its extension.

    Parameters
    ----------
    filename : str
        Name of the file to validate.
    expected_extension : str
        Expected file extension.

    Returns
    -------
    bool
        True if the file extension matches the expected extension, False otherwise.
    """

    # Split the file name from the extension, and compare the extracted extension with the expected extension parameter.
    _, ext = os.path.splitext(filename)
    return ext == expected_extension
    

def prep_csv(files, config):
    """Prepare CSV files by updating their filenames.

    Parameters
    ----------
    files : list of str
        List of filenames.
    config : dict
        Configuration dictionary.

    Returns
    -------
    list of dict
        A list of dictionaries containing local file paths and updated filenames.
    """
    # After we have validated the file type, we will join the csv_directory to the file name, then replace underscores with backslashes for upload to S3 later on.
    valid_files = list()
    for file in files:
        if validate_file_type(file, config['expected_file_extension']):
            local_filepath = os.path.join(config['csv_dir'], file)
            updated_filename = file.replace('_', '/')
            valid_files.append({local_filepath: updated_filename})
        else:
            print(f"{file} is incorrect file type. Expected '{config['expected_file_extension']}'")
    
    return valid_files
            
def id_and_prep_csvs(config):
    """Identify and prepare CSV files for processing.

    Parameters
    ----------
    config : dict
        Configuration dictionary.

    Returns
    -------
    list of dict
        A list of dictionaries containing local file paths and updated filenames.
    """
    # If the files are found, its prints the message and calls prep_csv to prepare the files. If not, prints an error message.
    try:
        files = list_files(config['csv_dir'])
        if files:
            print(f"Files found: {config['csv_dir']} directory.\n")
            return prep_csv(files, config)
        else:
            print(f"No files found in the {config['csv_dir']}")
            return []
    except OSError:
        print("Error accessing the directory.")
        
        
# Calls get_config to retrieve the configuration settings, and prints the config settings.            
if __name__ == "__main__":
    config = get_config()
    print(config)
# If directory exists, identify and prepare files.
    if check_directory_exists(config['csv_dir']):
        csv_info_list = id_and_prep_csvs(config)
        for csv_info in csv_info_list:
            print(csv_info)
    else:
        print("Directory does not exist. Terminating script.")

