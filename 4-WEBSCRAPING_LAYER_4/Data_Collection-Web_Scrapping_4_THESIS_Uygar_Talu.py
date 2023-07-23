########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

############################# PYTOK LIBRARY AND GENERATED LINKS ########################################################

###INFORMATION ABOUT THE SCRIPT(WEB SCRAPPING - 4)

"""
IN THE ANALYSIS LAYER WE HAVE 7 DIFFERENT FUNCTIONS.
1- "create_tiktok_links_for_pytok"
2- "capture_links_from_detailed_df_summary"
3- "remove_duplicates_from_csv"
4- "extract_video_paths"
5- "tiktok_webscrapper"
6- "execution_tiktok_scrapping"
7- "merge_metadata_vidoes"


Results for Web Scrapping 4- In this layer of analysis, We manipulate the created links in a form that pytok can scraps
and then create the csv file with the ready link structure for each of the hashtags and user profiles. Then by using
pytok library we start to scrap each column in our csv file where all the links are located. Both vidoes and the metadata
are scrapped by pytok and downloaded into a specified path in my machine. At the end we combine all the metadata for
all the videos.
"""

#FUNCTION 1: #FUNCTION TO CREATE THE VALID TIKTOK LINK VERSION FOR PYTOK
########################################################################################################################
##DESCRIPTION OF THE FUNCTION

#This function takes a list of TikTok video links and creates TikTok links compatible with PyTok by adding the necessary
#query parameters. It checks if the link already contains query parameters and appends the appropriate default addition
#if needed. The modified links are returned as a list.


def create_tiktok_links_for_pytok(list_of_links):
    """
        Creates TikTok links compatible with PyTok by adding necessary query parameters.

        Args:
            list_of_links (list): List of TikTok video links.

        Returns:
            list: List of TikTok links compatible with PyTok.
        """

    links_for_pytok = []

    for link in list_of_links:
        if "?" in link:
            default_addition = "is_copy_url=1&is_from_webapp=v1"
        else:
            default_addition = "?is_copy_url=1&is_from_webapp=v1"

        customized_link = link + default_addition
        links_for_pytok.append(customized_link)

    return links_for_pytok

########################################################################################################################
########################################################################################################################



#FUNCRION 2:  #FUNCTION TO CAPTURE THE LINKS FROM EACH COLUMN OF THE DETALIED LINK PRESENTATION DATA
#FILE WHICH WERE CREATED IN THE PREVIOUS STEP
########################################################################################################################
## DESCRIPTION OF THE FUNCTION

#This function captures TikTok links from each column of a detailed link presentation data file. It reads the CSV file
#specified by csv_file_path and iterates over each column. For each column, it retrieves the non-null values
#as a list and applies the previous function to create the proper links.

def capture_links_from_detailed_df_summary(csv_file_path):
    """
        Captures TikTok links from each column of the detailed link presentation data file.

        Args:
            csv_file_path (str): Path to the CSV file.

        Returns:
            dict: A dictionary containing lists of TikTok links captured from each column.
        """
    df = pd.read_csv(csv_file_path)
    column_lists = {}

    for column in df.columns:
        raw_links = df[column].dropna().tolist()
        column_lists[column] = create_tiktok_links_for_pytok(raw_links)

    return column_lists

########################################################################################################################
########################################################################################################################



#FUNCTION 3:  #FUNCTION TO DELETE THE DUPLICATE METADATA REGISTRATION BY PYTOK
########################################################################################################################
## DESCRIPTION OF THE FUNCTION

#This function removes duplicate entries from a CSV file. It reads the CSV file specified by csv_file_path, removes any
#duplicate rows, and overwrites the original file with the deduplicated data.

def remove_duplicates_from_csv(csv_file_path):
    """
        Removes duplicate entries from a CSV file.

        Args:
            csv_file_path (str): Path to the CSV file.
        """
    df = pd.read_csv(csv_file_path)
    df = df.drop_duplicates()
    df.to_csv(csv_file_path, index=False)

########################################################################################################################
########################################################################################################################



#FUNCTION 4: #FUNCTION TO CREATE VIDEO PATHS IN THE DIRECTORY BY CONSIDERING THE MANIPULATED VIDEO LINKS
########################################################################################################################
## DESCRIPTION OF THE EFUNCTION

# VIDEO PATH GENERATOR - USED IN (tiktok_webscrapper function)
#This function extracts the video path based on the manipulated video link. It takes a video URL as input, parses
#the video path from the URL, and constructs the complete video path by appending it to a default directory path.
#The function returns the full video path.


def extract_video_paths(video_url):
    """
        Extracts the video path based on the manipulated video link.

        Args:
            video_url (str): The manipulated video link.

        Returns:
            str: The video path.
        """
    default = "C:/Users/Uygar TALU/PycharmProjects/pythonProject1/SCRAPPED DATA FOR THESIS"
    sign = "@"
    video_path = video_url.split("@")[-1].split("?")[0]
    video_path = sign + video_path
    video_path = default + video_path
    video_path = video_path[::-1].replace("/", "_", 2)[::-1]
    video_path = video_path + ".mp4"
    return video_path

########################################################################################################################
########################################################################################################################




#FUNCTION 5:  #MORE AUTOMIZED VERSION OF TIKTOK WEBSCRAPPER - FUNCTION THAT AUTOMATICALLY REPORTS AND SCRAPS THE VIDEO
#LINKS FOR BOTH THE VIDEOS AND THE METADATA
########################################################################################################################
## DESCRIPTION OF THE FUNCTION

#This function is a more automated version of a TikTok web scraper. It scrapes TikTok videos and metadata based on the
#provided video URLs. For each video URL, it uses the PyTok library to save the TikTok video and metadata to a specified
#file. It also uses the extract_video_paths() function to generate the video paths based on the manipulated video links.
#The function returns a list of video paths for the scraped TikTok videos.

# ALL SCRAPPER FUNCTION - PATHS AND FORMAT
def tiktok_webscrapper(video_urls, column_name):
    """
        Scrape TikTok videos and metadata based on the provided video URLs.

        Args:
            video_urls (list): List of TikTok video URLs.
            column_name (str): Name of the column.

        Returns:
            list: List of video paths of the scraped TikTok videos.
        """
    video_paths = []
    error_count = 0  # Counter for problematic URLs

    base_file_name = "Thesis_TikTok_Data_Final_Scrapping"
    file_name = f"{base_file_name}_{column_name}.csv"

    print(f"Starting to scrap videos for column '{column_name}'. There are {len(video_urls)} videos to scrap.")

    # Get the current working directory
    original_directory = os.getcwd()

    # Change the working directory to my desired directory
    os.chdir("C:/Users/Uygar TALU/PycharmProjects/pythonProject1/SCRAPPED DATA FOR THESIS")

    for i, video_url in tqdm(enumerate(video_urls), total=len(video_urls), desc=f"Processing videos for {column_name}"):
        print(f"Processing video {i + 1} of {len(video_urls)}...")
        if video_url.startswith('http://') or video_url.startswith('https://'):
            try:
                # pass only the individual URL being processed, not all URLs
                if 'http' not in video_url:  # Check if the URL is valid
                    raise ValueError("Invalid URL")
                pyk.save_tiktok_multi_urls(video_urls=[video_url],
                                           save_video=True,
                                           metadata_fn=file_name,
                                           sleep=2,
                                           browser_name="chrome")
            except (KeyError, TypeError, ValueError, InvalidChunkLength) as e:
                print(f"An error of type {type(e).__name__} occurred while processing video '{video_url}'. Skipping this video.")
                error_count += 1  # Increment the error counter
                continue

            video_path = extract_video_paths(video_url)
            video_paths.append(video_path)
        else:
            print(f"Invalid URL: '{video_url}'. Skipping this video.")
            error_count += 1  # Increment the error counter
            continue

    print(f"Finished scrapping for column '{column_name}'.")
    print(f"Total problematic links: {error_count}")

    # Change back to the original directory
    os.chdir(original_directory)

    return video_paths

########################################################################################################################
########################################################################################################################


#FUNCTION 6: EXECUTION FUNCTION THAT CONSIDERS THE ABOVE SUPPLEMENTARY FUNCTIONS -  EXECUTES THE WHOLE SCRAPPING
#PROCESS AND STORES THE METADATA AND VIDEOS IN THE DIRECTORY - Full Automation
########################################################################################################################
## DESCRIPTION OF THE FUNCTION

#This function executes the TikTok scraping process for each column of a detailed link presentation data file. It takes
#the path to the CSV file and an optional start column as input. It captures the links from the detailed data file,
#performs TikTok scraping for each column, saves the video paths, removes duplicate metadata entries, and asks the
#user if they want to continue with the next column. Main reason of this question ot he user is that, since the
#execution function performs a lot of tasks as the data scraped increases the burden of computational load becomes
#higher for the machine so that when the function asks this question, user can have time to copy the videos scraped to
#another disc save it there to decrease the computational load.

def execution_tiktok_scrapping(csv_file_path, start_column=None):
    """
        Executes the TikTok scrapping process for each column of a detailed link presentation data file.

        Args:
            csv_file_path (str): The path to the CSV file containing the detailed link presentation data.
            start_column (str, optional): The name of the column to start the scrapping process from. Default is None.

        Returns:
            list: A list of video paths for the scraped TikTok videos.
        """

    ready_links_dict = capture_links_from_detailed_df_summary(csv_file_path)
    video_paths = []
    start_processing = False

    for column, links in ready_links_dict.items():
        # If a start column has been defined, skip columns until we reach it
        if start_column:
            if column == start_column:
                start_processing = True
            elif not start_processing:
                continue

        video_paths.append(tiktok_webscrapper(video_urls=links, column_name=column))
        print(f"Finished scrapping for column '{column}'.")

        # Cleaning up the CSV file after each scrapping process
        remove_duplicates_from_csv(f"C:/Users/Uygar TALU/PycharmProjects/pythonProject1/SCRAPPED DATA FOR THESIS/Thesis_TikTok_Data_Final_Scrapping_{column}.csv")

        # Asking the user if they want to continue
        answer = input("Do you want to continue with the next column? (yes/no): ")

        if answer.lower() != "yes":
            break  # Stopping processing columns

    return video_paths


path = "C:/Users/Uygar TALU/Desktop/WEBSCRAPPING_THESIS/METADA DIVIDED/All Metadata CSV Files - STANDARDIZED - ANOMALY DETECTED/ANOMALY-DETECTED-LINKS.csv"
start_column = "#14mayıs"
video_paths_to_be_analyzed = execution_tiktok_scrapping(csv_file_path=path, start_column=start_column)

"""
ALL THIS PROCESS IS EXECUTED AUTOMATICALLY UP TO SOME DEGREE. IN THE CURRENT DIRECTORY USER NEEDS TO CONTROL AND 
REARRANGE THE LOCATIONS OF THE SCRAPPED VIDEOS AND THE CSV FILES WHICH CONTAIN THE METADATA OF THE VIDEOS 
"""
########################################################################################################################
########################################################################################################################



#FUNCTION 7: #FUNCTION TO COMBINE ALL THE METADATA FORM THE EACH HASHTAG AND USER PROFILE
########################################################################################################################
## DESCRIPTION OF THE FUNCTION

#This function merges multiple CSV files containing metadata into a single CSV file. It takes the path to the directory
#containing the CSV files and the output file name as input. It reads each CSV file in the directory, concatenates them
#into a single DataFrame, and saves the merged DataFrame as a CSV file.


def merge_metadata_vidoes(path, output_file_name):
    """
        Merges multiple CSV files containing metadata into a single CSV file.

        Args:
            path (str): The path to the directory containing the CSV files.
            output_file_name (str): The name of the output merged CSV file.

        Returns:
            None
        """

    dfs = []

    csv_files = [f for f in os.listdir(path) if f.endswith('.csv')]

    csv_files.sort()

    for file_name in csv_files:
        # Create the full file path
        file_path = os.path.join(path, file_name)
        # Read the csv file and append it to the dfs list
        dfs.append(pd.read_csv(file_path))

    df = pd.concat(dfs, ignore_index=True)
    df.to_csv(os.path.join(path, output_file_name), index=False)



path_metadata = "C:\\Users\\Uygar TALU\\Desktop\\WEBSCRAPPING_THESIS\\METADA DIVIDED\\All Metadata CSV Files"
merged_metadata_file_name = "All_Data_Scrapped_Metadata_THESIS.csv"
merge_metadata_vidoes(path=path_metadata, output_file_name=merged_metadata_file_name)

########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
