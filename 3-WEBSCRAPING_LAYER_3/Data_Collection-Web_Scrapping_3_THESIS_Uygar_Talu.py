########################################################################################################################
########################################################################################################################
########################################################################################################################
############################## CREATING FINAL READY LINK FILES IN 4 DIFFERENT VERSIONS #################################

###INFORMATION ABOUT THE SCRIPT(WEB SCRAPPING - 3)

"""
IN THE ANALYSIS LAYER WE HAVE 4 DIFFERENT FUNCTIONS.
1- "final_tiktoklinks_file_creator_filetype_1"
2- "final_tiktoklinks_file_creator_filetype_2_PROFILE"
3- "final_tiktoklinks_file_creator_filetype_3_HASHTAG"
4- "detailed_tiktok_links_data_file_creator"

Results for Web Scrapping 3- In this layer of analysis, we used 4 different functions to combine all the links
created in the previous layers. With 4 different functions we create 4 different csv files where they all contain the
video links with different divisions. (For hashtags, for user profiles, everything included hashtag by hashtag and user
profile by user profile and just one column and all the links are in the below.)
"""

#FUNCTION 1:  #FINAL DATA FILE CREATOR FOR ALL THE LINKS
########################################################################################################################
## DESCRIPTION OF THE FUNCTION:

#It creates a final CSV file that consolidates TikTok video links from multiple separate CSV files.
#It prepares the file paths by converting backslashes to forward slashes and creates a list of these ready file paths.
#It reads each separate CSV file, extracts the video links, and appends them to a list.
#It saves the consolidated video links as a new CSV file on the desktop.
#It checks if the total number of rows in the final CSV file matches the expected number based on the separate CSV files.
#It returns the consolidated video links as a DataFrame, the list of all video links, and the list of ready file paths.

def final_tiktoklinks_file_creator_filetype_1():
    """
        Creates a final CSV file that contains all the TikTok video links from separate CSV files.

        Returns:
            pd.DataFrame: The final dataframe containing all the video links.
            pd.DataFrame: The dataframe with all the video links.
            list: The list of paths to the separate CSV files.
        """

    import pandas as pd
    import os

    all_paths_list = [r"C:\Users\Uygar TALU\Desktop\rtedijital_SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\akparti_SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\akgenclikgm_SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\akpartiistanbul34_SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\chptiktok_SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\kemalkilicdaroglu_SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\ekrem.imamoglu_SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\mustafasarigul__SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\yavasmansur_SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\genel_secim_2023_SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\secimhatti_SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\secim_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\secim2023_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\herşeyçokgüzelolacak_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\benkemalgeliyorum_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\sanasöz_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\dogruadam_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\türkiyeyüzyılı_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\hedef2023_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\14mayıs_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\28mayıs_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\mevzularaçıkmikrofon_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\reis_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\akp_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\akparti_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\chp_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\milletittifakı_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\cumhurbaskani_SCRAPPED_VIDEO_LINKS_HASHTAG.csv"]

    ready_paths_list = []
    all_links_list = []

    for path in all_paths_list:
        ready_path = path.replace("\\", "/")
        ready_paths_list.append(ready_path)

    print(f"CSV FILE PATHS ARE READY: {ready_paths_list}")

    total_rows = 0

    for ready_path in ready_paths_list:
        data = pd.read_csv(ready_path)
        all_links_list.extend(data.iloc[:,0].tolist())
        total_rows += len(data)

        print(f"{ready_path} IS ADDED INTO THE GENERAL LIST")
        print(f"TOTAL NUMBER OF LINKS FOR THE FILE - {len(data)}")


    addition_1 = "ALL_LINKS_TO_BE_SCRAPPED_PROFILE_AND_HASHTAG"
    addition_2 = "File_1"
    file_name = addition_1 + addition_2
    desktop_path = os.path.expanduser("~/Desktop")
    csv_file_path = os.path.join(desktop_path, file_name + ".csv")
    all_links_list = pd.DataFrame(all_links_list, columns=["Links To Be Scrapped"])

    all_links_list.to_csv(csv_file_path, index=False)
    print(f"{csv_file_path} IS CONVERTED INTO CSV FORMAT AND DOWNLOADED INTO DESKTOP")

    csv_file_path = csv_file_path.replace("\\", "/")
    All_Links_To_Be_Scrapped = pd.read_csv(csv_file_path)

    print(f"IN TOTAL {len(ready_paths_list)} CSV FILES(PROFILES & HASHTAGS)")
    print(f"FINAL DATAFRAME {file_name} IMPORTED AS CSV: {csv_file_path}")
    print(f"FINAL DATAFRAME {file_name} HAS {len(all_links_list)} LINKS")
    print(f"IN TOTAL THERE ARE {len(all_links_list)} LINKS TO BE SCRAPPED")

    if total_rows == len(All_Links_To_Be_Scrapped):
        print("EVERYTHING IS FINE GOOD TO SCRAP METADATA")
    else:
        print("ERROR:TOTAL NUMBER OF LINKS IN THE FINAL CSV IS NOT INLINE WITH THE SEPERATE CSV FILES")

    return All_Links_To_Be_Scrapped, all_links_list, ready_paths_list

########################################################################################################################
########################################################################################################################


#FUNCTION 2:  #FINAL DATA FILE CREATOR FOR THE PROFILE LINKS
########################################################################################################################
## DESCRIPTION OF THE FUNCTION

#It creates a final CSV file that consolidates TikTok video links from separate CSV files.
#It prepares the file paths by converting backslashes to forward slashes and creates a list of these ready file paths.
#It reads each separate CSV file, extracts the video links, and appends them to a list.
#It saves the consolidated video links as a new CSV file on the desktop.
#It checks if the total number of rows in the final CSV file matches the expected number based on the separate CSV files.
#It returns the consolidated video links as a DataFrame, the list of all video links, and the list of ready file paths.

def final_tiktoklinks_file_creator_filetype_2_PROFILE():
    """
        Creates a final CSV file that contains all the TikTok video links from separate CSV files.

        Returns:
            pd.DataFrame: The final dataframe containing all the video links.
            pd.DataFrame: The dataframe with all the video links.
            list: The list of paths to the separate CSV files.
        """

    import pandas as pd
    import os

    all_paths_list = [r"C:\Users\Uygar TALU\Desktop\rtedijital_SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\akparti_SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\akgenclikgm_SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\akpartiistanbul34_SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\chptiktok_SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\kemalkilicdaroglu_SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\ekrem.imamoglu_SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\mustafasarigul__SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\yavasmansur_SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\genel_secim_2023_SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\secimhatti_SCRAPPED_VIDEO_LINKS.csv"]

    ready_paths_list = []
    all_links_list = []

    for path in all_paths_list:
        ready_path = path.replace("\\", "/")
        ready_paths_list.append(ready_path)

    print(f"CSV FILE PATHS ARE READY: {ready_paths_list}")

    total_rows = 0

    for ready_path in ready_paths_list:
        data = pd.read_csv(ready_path)
        all_links_list.extend(data.iloc[:,0].tolist())
        total_rows += len(data)

        print(f"{ready_path} IS ADDED INTO THE GENERAL LIST")
        print(f"TOTAL NUMBER OF LINKS FOR THE FILE - {len(data)}")


    addition_1 = "ALL_LINKS_TO_BE_SCRAPPED_PROFILE"
    addition_2 = "File_2"
    file_name = addition_1 + addition_2
    desktop_path = os.path.expanduser("~/Desktop")
    csv_file_path = os.path.join(desktop_path, file_name + ".csv")
    all_links_list = pd.DataFrame(all_links_list, columns=["Links To Be Scrapped"])

    all_links_list.to_csv(csv_file_path, index=False)
    print(f"{csv_file_path} IS CONVERTED INTO CSV FORMAT AND DOWNLOADED INTO DESKTOP")

    csv_file_path = csv_file_path.replace("\\", "/")
    All_Links_To_Be_Scrapped = pd.read_csv(csv_file_path)

    print(f"IN TOTAL {len(ready_paths_list)} CSV FILES(PROFILES & HASHTAGS)")
    print(f"FINAL DATAFRAME {file_name} IMPORTED AS CSV: {csv_file_path}")
    print(f"FINAL DATAFRAME {file_name} HAS {len(all_links_list)} LINKS")
    print(f"IN TOTAL THERE ARE {len(all_links_list)} LINKS TO BE SCRAPPED")

    if total_rows == len(All_Links_To_Be_Scrapped):
        print("EVERYTHING IS FINE GOOD TO SCRAP METADATA")
    else:
        print("ERROR:TOTAL NUMBER OF LINKS IN THE FINAL CSV IS NOT INLINE WITH THE SEPERATE CSV FILES")

    return All_Links_To_Be_Scrapped, all_links_list, ready_paths_list

########################################################################################################################
########################################################################################################################

#FUNCTION 3: #FINAL DATA FILE CREATOR FOR THE HASHTAG LINKS
########################################################################################################################
## DESCRIPTION OF THE FUNCTION

#It creates a final CSV file that consolidates TikTok video links from separate CSV files.
#It prepares the file paths by converting backslashes to forward slashes and creates a list of these ready file paths.
#It reads each separate CSV file, extracts the video links, and appends them to a list.
#It saves the consolidated video links as a new CSV file on the desktop.
#It checks if the total number of rows in the final CSV file matches the expected number based on the separate CSV files.
#It returns the consolidated video links as a DataFrame, the list of all video links, and the list of ready file paths.

def final_tiktoklinks_file_creator_filetype_3_HASHTAG():
    """
        Creates a final CSV file that contains all the TikTok video links from separate CSV files.

        Returns:
            pd.DataFrame: The final dataframe containing all the video links.
            pd.DataFrame: The dataframe with all the video links.
            list: The list of paths to the separate CSV files.
        """

    import pandas as pd
    import os

    all_paths_list = [r"C:\Users\Uygar TALU\Desktop\secim_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\secim2023_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\herşeyçokgüzelolacak_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\benkemalgeliyorum_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\sanasöz_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\dogruadam_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\türkiyeyüzyılı_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\hedef2023_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\14mayıs_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\28mayıs_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\mevzularaçıkmikrofon_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\reis_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\akp_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\akparti_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\chp_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\milletittifakı_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\cumhurbaskani_SCRAPPED_VIDEO_LINKS_HASHTAG.csv"]

    ready_paths_list = []
    all_links_list = []

    for path in all_paths_list:
        ready_path = path.replace("\\", "/")
        ready_paths_list.append(ready_path)

    print(f"CSV FILE PATHS ARE READY: {ready_paths_list}")

    total_rows = 0

    for ready_path in ready_paths_list:
        data = pd.read_csv(ready_path)
        all_links_list.extend(data.iloc[:,0].tolist())
        total_rows += len(data)

        print(f"{ready_path} IS ADDED INTO THE GENERAL LIST")
        print(f"TOTAL NUMBER OF LINKS FOR THE FILE - {len(data)}")


    addition_1 = "ALL_LINKS_TO_BE_SCRAPPED_HASHTAG"
    addition_2 = "File_3"
    file_name = addition_1 + addition_2
    desktop_path = os.path.expanduser("~/Desktop")
    csv_file_path = os.path.join(desktop_path, file_name + ".csv")
    all_links_list = pd.DataFrame(all_links_list, columns=["Links To Be Scrapped"])

    all_links_list.to_csv(csv_file_path, index=False)
    print(f"{csv_file_path} IS CONVERTED INTO CSV FORMAT AND DOWNLOADED INTO DESKTOP")

    csv_file_path = csv_file_path.replace("\\", "/")
    All_Links_To_Be_Scrapped = pd.read_csv(csv_file_path)

    print(f"IN TOTAL {len(ready_paths_list)} CSV FILES(PROFILES & HASHTAGS)")
    print(f"FINAL DATAFRAME {file_name} IMPORTED AS CSV: {csv_file_path}")
    print(f"FINAL DATAFRAME {file_name} HAS {len(all_links_list)} LINKS")
    print(f"IN TOTAL THERE ARE {len(all_links_list)} LINKS TO BE SCRAPPED")

    if total_rows == len(All_Links_To_Be_Scrapped):
        print("EVERYTHING IS FINE GOOD TO SCRAP METADATA")
    else:
        print("ERROR:TOTAL NUMBER OF LINKS IN THE FINAL CSV IS NOT INLINE WITH THE SEPERATE CSV FILES")

    return All_Links_To_Be_Scrapped, all_links_list, ready_paths_list

########################################################################################################################
########################################################################################################################


#FUNCTION 4:  #FINAL DATA FILE CREATOR FOR MORE DETAILED TYPE(DIVIDED VERSION)
########################################################################################################################
## DESCRIPTION OF THE FUNCTION

#It prepares a list of file paths for separate CSV files containing TikTok video links.
#It converts the file paths by replacing backslashes with forward slashes and creates a list of these ready file paths.
#It extracts the name column for each file path using regular expressions and appends it to a list of column names.
#It reads each CSV file, assigns the extracted name column as the column name, and merges the dataframes into a single dataframe.
#It prints the number of columns in the resulting dataframe, representing hashtags and profiles.
#It calculates the total number of rows and prints the number of rows for each column, indicating the count of links.
#It calculates the percentage of links in each column compared to the total number of links and prints it.
#It saves the resulting dataframe as a CSV file on the desktop.
#It reads the saved CSV file and assigns it to the variable DETAILED_LINKS_DATA_FILE.
#It returns the DETAILED_LINKS_DATA_FILE dataframe.


def detailed_tiktok_links_data_file_creator():
    """
       Creates a detailed CSV file that contains all the TikTok video links from separate CSV files.

       Returns:
           pd.DataFrame: The detailed dataframe containing all the video links.
       """

    import re
    all_paths_list = [r"C:\Users\Uygar TALU\Desktop\rtedijital_SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\akparti_SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\akgenclikgm_SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\akpartiistanbul34_SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\chptiktok_SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\kemalkilicdaroglu_SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\ekrem.imamoglu_SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\mustafasarigul__SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\yavasmansur_SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\genel_secim_2023_SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\secimhatti_SCRAPPED_VIDEO_LINKS.csv",
                      r"C:\Users\Uygar TALU\Desktop\secim_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\secim2023_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\herşeyçokgüzelolacak_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\benkemalgeliyorum_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\sanasöz_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\dogruadam_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\türkiyeyüzyılı_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\hedef2023_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\14mayıs_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\28mayıs_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\mevzularaçıkmikrofon_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\reis_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\akp_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\akparti_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\chp_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\milletittifakı_SCRAPPED_VIDEO_LINKS_HASHTAG.csv",
                      r"C:\Users\Uygar TALU\Desktop\cumhurbaskani_SCRAPPED_VIDEO_LINKS_HASHTAG.csv"]

    ready_paths_list = []
    column_names = []
    general_df_links = pd.DataFrame()

    for path in all_paths_list:
        ready_path = path.replace("\\", "/")
        ready_paths_list.append(ready_path)

    print(f"CSV FILE PATHS ARE READY: {ready_paths_list}")

    for path in ready_paths_list:
        pattern = r"[\\/](?!.*[\\/])([^_]+)_.*"
        result = re.search(pattern, path)
        if result:
            name_column = result.group(1)
            column_names.append(name_column)

    for ready_path, column_name in zip(ready_paths_list, column_names):
        data = pd.read_csv(ready_path)
        data.columns = [column_name]  # Here I am renaming the column to column_name
        if general_df_links.empty:  # If general_df_links is empty, we simply assign the first DataFrame to it
            general_df_links = data
        else:  # If general_df_links is not empty, I merge dataframes on the index
            general_df_links = pd.merge(general_df_links, data, left_index=True, right_index=True, how='outer')

    print(f"DETAILED DATAFRAME HAS {len(general_df_links.columns)} COLUMNS WHICH CORRESPOND TO HASHTAGS AND PROFILES")

    total_rows = 0
    for column in general_df_links.columns:
        num_rows = general_df_links[column].count()
        total_rows += num_rows

        print(f"THE COLUMN '{column}' HAS {num_rows} ROWS.")

    print(f"IN TOTAL, THE DATA FRAME HAS {total_rows} LINKS TO BE SCRAPPED.")

    for column in general_df_links.columns:
        num_rows_spec_column = general_df_links[column].count()
        percentage = (num_rows_spec_column / total_rows) * 100

        print(f"THE COLUMN '{column}' HAS {percentage:.2f}% OF THE TOTAL LINKS.")

    addition_1 = "DETAILED_LINKS_DATA_FILE"
    addition_2 = "File_4"
    file_name = addition_1 + addition_2
    desktop_path = os.path.expanduser("~/Desktop")
    csv_file_path = os.path.join(desktop_path, file_name + ".csv")
    general_df_links.to_csv(csv_file_path, index=False)

    print(f"{csv_file_path} IS CONVERTED INTO CSV FORMAT AND DOWNLOADED INTO DESKTOP")

    csv_file_path = csv_file_path.replace("\\", "/")
    DETAILED_LINKS_DATA_FILE = pd.read_csv(csv_file_path)

    return DETAILED_LINKS_DATA_FILE

############################# EXECUTION TO CREATE FINAL READY LINK CONTAINED CSV FILES #################################

#CSV FILE FOR ALL LINKS TOGETHER - USE THIS IN PYTOK FUNCTION
All_Links_To_Be_Scrapped, all_links_list, ready_paths_list = final_tiktoklinks_file_creator_filetype_1()

#CSV FILE FOR PROFILE LINKS ONLY
All_Links_To_Be_Scrapped_PROFILE, all_links_list_PROFILE, ready_paths_list_PROFILE = final_tiktoklinks_file_creator_filetype_2_PROFILE()

#CSV FILE FOR HASHTAG LINKS ONLY
All_Links_To_Be_Scrapped_HASHTAG, all_links_list_HASHTAG, ready_paths_list_HASHTAG = final_tiktoklinks_file_creator_filetype_3_HASHTAG()

#CSV FILE FOR DETAILED DATA REPRESENTATION
DETAILED_LINKS_DATA_FILE = detailed_tiktok_links_data_file_creator()
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################






