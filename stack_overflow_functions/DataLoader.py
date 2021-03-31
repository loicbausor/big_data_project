# Data downloader module
import os
import patoolib
import gdown
import shutil
import geopandas as gpd 

def clear_gdown_cache():
    gdown_cache = os.environ['HOME'] + "/.cache/gdown"
    if os.path.exists(gdown_cache):
        shutil.rmtree(gdown_cache)

def download_data():
    """
    Download the sampled data from the the drive : 
    https://drive.google.com/uc?id=12_5fYiJBSdb291XbWhIApNRF8kMrGe-4
    And extracts its data in the folder Data/sample
    """
    # Ensure that that the data has not already be preprared
    path_exists = []
    tags_out_file = "Data/sample/Tags"
    post_out_file = "Data/sample/Posts"
    badge_out_file = "Data/sample/Badges"
    user_out_file = "Data/sample/Users"
    location_out_file = "Data/sample/Country"
    
    list_dir = [
        tags_out_file,
        post_out_file,
        badge_out_file,
        user_out_file,
        location_out_file
    ]
    
    # Checks the directories 
    for dirs in list_dir:
        if not os.path.exists(dirs):
            path_exists.append(False)
        else:
            path_exists.append(True)
    
    path_dict = {dirs : exists 
        for dirs, exists in zip(list_dir, path_exists)
                }
    
    ## Verifies if all the data already exists
    if sum(path_exists) == len(list_dir):
        print("All the data folder already exists, we infer that you have " + 
        "already downloaded or extracted the data. " + 
        "If it is not the case delete the data folder " +
        "(Data/sample) and re run this function.")
        return "Done, data can be found at {}/Data/sample.".format(os.environ["HOME"])
    
    ## Raise an exception if some pathes exists and other don't
    elif sum(path_exists) != 0:
        doubtful_pathes = []
        non_existing_pathes = []
        for dirs, exists in path_dict.items():
            if exists:
                doubtful_pathes.append(dirs)
            else:
                non_existing_pathes.append(dirs)
        raise Exception("Folders {} exists but not {} ... You do not have all the data necessary.\
        Delete those directory and re run this function to make sure everything is fine."
        .format(doubtful_pathes, non_existing_pathes)) 
    
    # Downloads and extracts the data
    output = 'Data/sample.rar'
    url = "https://drive.google.com/uc?id=12_5fYiJBSdb291XbWhIApNRF8kMrGe-4"
    
    print("Creates the folder.")
    if not os.path.exists('Data/sample'):
        os.makedirs('Data/sample')
    
    print("Starts the download of the data.")
    clear_gdown_cache()
    gdown.download(url, output, quiet=False)
    
    print("Starts the extract of the data.")
    patoolib.extract_archive(output, outdir="Data/sample")
    os.remove(output)
    
    return "Done, data can be found at {}/Data/sample.".format(os.environ["HOME"])
    
    
def get_country_polygon():
    """
    Download the sampled data from the the drive : 
    https://drive.google.com/file/d/1wMcVPXaoGdoR03jd1A-uQQ48NpY87mLT/
    , extracts its data and returns the dataframe associated
    """
    # Downloads and extracts the data
    
    output = 'tmp.rar'
    url = "https://drive.google.com/uc?id=1wMcVPXaoGdoR03jd1A-uQQ48NpY87mLT" 
    
    clear_gdown_cache()
    gdown.download(url, output, quiet=True)
    patoolib.extract_archive(output, verbosity=-1)
    os.remove(output)
    shapefile = "country_polygons/ne_110m_admin_0_countries.shp"
    geo_data = gpd.read_file(shapefile)[['ADM0_A3', 'geometry']]
    shutil.rmtree("country_polygons")
    return geo_data