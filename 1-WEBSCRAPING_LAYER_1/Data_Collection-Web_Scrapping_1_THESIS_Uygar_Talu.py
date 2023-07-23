########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

#################################### WEBSCRAPPING FOR USER PROFILES - 1 ###############################################

###INFORMATION ABOUT THE SCRIPT(WEB SCRAPPING - 1)

"""
IN THE ANALYSIS LAYER 1 WE HAVE 4 DIFFERENT FUNCTIONS.
1- "auto_data_file_importer"
2- "extract_video_ids"
3- "tiktok_link_generator_profile"
4- "save_videolinks_to_csv"

Results for Web Scrapping 1- In this layer of analysis, we first use Zeeschuimer to scrap the data objects form user
profiles and hashtags manually by scanning, then we capture the unique video ids from the downloaded ndjson files.
Depending on the unique video ids we created the TikTok link structure for each video. In the resulting dataframe we
have labels as hashtag videos and user profiles. In the rows we have related video links.

Label: Hashtag or user profile label for the processed video.
"""
########################################################################################################################
########################################################################################################################


#FUNCTION 1: #AUTOMATIC NDJSON CONVERTER TO CSV AND IMPORT INTO PYTHON ENV
########################################################################################################################

#Imports a data file in ndjson format for a given username.
#Converts the imported data into a pandas DataFrame.
#Saves the DataFrame as a CSV file.
#Returns the imported data as a DataFrame.


def auto_data_file_importer(username):
    """
    Imports the data file in ndjson format for a given username and converts it to a CSV file.

    Args:
        username (str): The username for which the data file will be imported.

    Returns:
        pd.DataFrame: The imported data in the form of a pandas DataFrame.

    """
    import pandas as pd
    import csv
    import json

    default_ndjson_path_1 = "C:/Users/Uygar TALU/Desktop/"
    default_ndjson_path_format_1 = ".ndjson"
    default_ndjson_path_format_2 = ".csv"
    default_path_addition = "_SCRAPPED"

    ndjson_file_path_local = default_ndjson_path_1 + username + default_path_addition + default_ndjson_path_format_1
    csv_file_path_local = default_ndjson_path_1 + username + default_path_addition + default_ndjson_path_format_2

    with open(ndjson_file_path_local, 'r', encoding='utf-8') as ndjson_file:
        data = [json.loads(line) for line in ndjson_file]

    header = data[0].keys()

    with open(csv_file_path_local, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=header)
        writer.writeheader()
        writer.writerows(data)

    df = pd.read_csv(csv_file_path_local)

    num_rows = len(df)
    print(f"NUMBER OF ROWS: {num_rows} rows.")
    print(f"DATA FILE TYPE: {type(df)}")

    if isinstance(df, pd.core.frame.DataFrame):
        print("SUCCESSFULLY IMPORTED - READY TO GO")
    else:
        print(f"DATA TYPE IS WRONG. CURRENT DATA TYPE IS: {type(df)}")

    return df

########################################################################################################################
########################################################################################################################


#FUNCTION 2: #CAPTURING UNIQUE VIDEO IDS FOR THE ACCOUNT PROFILE
########################################################################################################################

#Extracts the video IDs from a dataframe.
#Iterates over the 'data' column of the DataFrame.
#Converts each row into a dictionary.
#Extracts the 'id' value from the dictionary as the video ID.
#Adds unique video IDs to a list.
#Returns the list of video IDs.


def extract_video_ids(dataframe):
    """
    Extracts the video IDs from a dataframe.

    Args:
        dataframe (pd.DataFrame): The dataframe containing the 'data' column.

    Returns:
        list: A list of video IDs.

    """
    video_ids = []
    for row in dataframe['data']:
        try:
            row_dict = ast.literal_eval(row)
            video_id = row_dict['id']
            if video_id not in video_ids:
                video_ids.append(video_id)
        except (ValueError, KeyError):
            continue
    return video_ids

########################################################################################################################
########################################################################################################################


#FUNCTION 3:  #AUTO TIKTOK VIDEO LINK CREATOR FOR PYTOK WEB SCRAPPING
########################################################################################################################

#Generates TikTok video links for a given username and a list of video IDs.
#Iterates over the video IDs and constructs the TikTok video link using the username and video ID.
#Appends the generated link to a list.
#Prints each generated video link.
#Returns the list of generated TikTok video links.

def tiktok_link_generator_profile(username, video_ids_list):
    """
    Generates TikTok video links for a given username and a list of video IDs.

    Args:
        username (str): The username for which the video links will be generated.
        video_ids_list (list): A list of video IDs.

    Returns:
        list: A list of generated TikTok video links.

    """
    captured_links = []

    default_structure_1 = "https://www.tiktok.com/"
    default_structure_2 = "/video/"

    for i, id in enumerate(video_ids_list, start=1):
        valid_link = default_structure_1 + username + default_structure_2 + id
        captured_links.append(valid_link)
        print(f"VIDEO URL {i} is created: {valid_link}")

    total_links_created = len(captured_links)
    print(f"IN TOTAL {total_links_created} LINKS ARE CREATED FOR {username}")

    return captured_links

########################################################################################################################
########################################################################################################################


#FUNCTION 4:  #CREATED VIDEO LINK SAVING FUNCTION INTO CSV
########################################################################################################################

#Takes a list of video links and a username as input.
#Saves the video links to a CSV file.
#Useful for storing and organizing video links associated with a particular username.


def save_videolinks_to_csv(video_links_list, username):
    """
    Saves the generated video links to a CSV file.

    Args:
        video_links_list (list): A list of video links.
        username (str): The username associated with the video links.

    """
    import pandas as pd
    import os

    addition_1 = "_SCRAPPED"
    addition_2 = "_VIDEO_LINKS"
    file_name = username + addition_1 + addition_2

    desktop_path = os.path.expanduser("~/Desktop")
    csv_file_path = os.path.join(desktop_path, file_name + ".csv")

    video_links_list = pd.DataFrame(video_links_list)

    video_links_list.to_csv(csv_file_path, index=False)
    print(f"DataFrame {file_name} saved as CSV: {csv_file_path}")

save_videolinks_to_csv(video_links_list=video_links_rtedijital_profile,
                       username=username)
########################################################################################################################
########################################################################################################################
########################################################################################################################



#### EXECUTION OF THE FUNCTIONS
########################################################################################################################
########################################################################################################################
############ EXECUTION FOR PROFILE VIDEOS - SCRAPPING BOTH THE VIDEOS AND METADATA AS CSV FILE #########################

#######1- SCRAPPING FOR @RTEDIJITAL - DONE
username = "rtedijital"
rtedijital_dataframe_scrapped = auto_data_file_importer(username=username)

video_ids_rtedijital = extract_video_ids(rtedijital_dataframe_scrapped)

username_spec = "@rtedijital"
video_links_rtedijital_profile = tiktok_link_generator(video_ids_list=video_ids,
                                                       username=username_spec)

save_videolinks_to_csv(video_links_list=video_links_rtedijital_profile,
                       username=username)


#######2- SCRAPPING FOR @AKPARTI  - DONE
username = "akparti"
akparti_dataframe_scrapped = auto_data_file_importer(username=username)

video_ids_akparti = extract_video_ids(dataframe=akparti_dataframe_scrapped)

username_spec = "@akparti"
video_links_akparti_profile = tiktok_link_generator_profile(username=username_spec,
                                                            video_ids_list=video_ids_akparti)

save_videolinks_to_csv(video_links_list=video_links_akparti_profile,
                       username=username)


#######3- SCRAPPING FOR @akpgenclikgm - DONE
username = "akgenclikgm"
akgenclikgm_dataframe_scrapped = auto_data_file_importer(username=username)

video_ids_akgenclikgm = extract_video_ids(dataframe=akgenclikgm_dataframe_scrapped)

username_spec = "@akgenclikgm"
video_links_akgenclikgm_profile = tiktok_link_generator_profile(username=username_spec,
                                                                video_ids_list=video_ids_akgenclikgm)

save_videolinks_to_csv(video_links_list=video_links_akgenclikgm_profile,
                       username=username)


#######4- SCRAPPING FOR @akpartiistanbul34 - DONE
username = "akpartiistanbul34"
akpartiistanbul34_dataframe_scrapped = auto_data_file_importer(username=username)

video_ids_akpartiistanbul34 = extract_video_ids(dataframe=akpartiistanbul34_dataframe_scrapped)

username_spec = "@akpartiistanbul34"
video_links_akpartiistanbu34_profile = tiktok_link_generator_profile(username=username_spec,
                                                                     video_ids_list=video_ids_akpartiistanbul34)

save_videolinks_to_csv(video_links_list=video_links_akpartiistanbu34_profile,
                       username=username)


#######5- SCRAPPING FOR @chptiktok -  DONE
username = "chptiktok"
chptiktok_dataframe_scrapped = auto_data_file_importer(username=username)

video_ids_chptiktok = extract_video_ids(dataframe=chptiktok_dataframe_scrapped)

username_spec = "@chptiktok"
video_links_chptiktok_profile = tiktok_link_generator_profile(username=username_spec,
                                                              video_ids_list=video_ids_chptiktok)

save_videolinks_to_csv(video_links_list=video_links_chptiktok_profile,
                       username=username)


#######6- SCRAPPING FOR @kemalkilicdaroglu - DONE
username = "kemalkilicdaroglu"
kemalkilicdaroglu_dataframe_scrapped = auto_data_file_importer(username=username)

video_ids_kemalkilicdaroglu = extract_video_ids(dataframe=kemalkilicdaroglu_dataframe_scrapped)

username_spec = "@kemalkilicdaroglu"
video_links_kemalkilicdaroglu_profile = tiktok_link_generator_profile(username=username_spec,
                                                                      video_ids_list=video_ids_kemalkilicdaroglu)

save_videolinks_to_csv(video_links_list=video_links_kemalkilicdaroglu_profile,
                       username=username)


#######7- SCRAPPING FOR @yavasmansur - DONE
username = "yavasmansur"
yavasmansur_dataframe_scrapped = auto_data_file_importer(username=username)

video_ids_yavasmansur = extract_video_ids(dataframe=yavasmansur_dataframe_scrapped)

username_spec = "@yavasmansur"
video_links_yavasmansur_profile = tiktok_link_generator_profile(username=username_spec,
                                                                video_ids_list=video_ids_yavasmansur)

save_videolinks_to_csv(video_links_list=video_links_yavasmansur_profile,
                       username=username)


#######8- SCRAPPING FOR @ekrem.imamoglu - DONE
username = "ekrem.imamoglu"
ekremimamoglu_dataframe_scrapped = auto_data_file_importer(username=username)

video_ids_ekremimamoglu = extract_video_ids(dataframe=ekremimamoglu_dataframe_scrapped)

username_spec = "@ekrem.imamoglu"
video_links_ekremimamoglu_profile = tiktok_link_generator_profile(username=username_spec,
                                                                video_ids_list=video_ids_ekremimamoglu)

save_videolinks_to_csv(video_links_list=video_links_ekremimamoglu_profile,
                       username=username)


#######9- SCRAPPING FOR @mustafasarigul_ - DONE
username = "mustafasarigul_"
mustafasarigul_dataframe_scrapped = auto_data_file_importer(username=username)

video_ids_mustafasarigul = extract_video_ids(dataframe=mustafasarigul_dataframe_scrapped)

username_spec = "@mustafasarigul_"
video_links_mustafasarigul_profile = tiktok_link_generator_profile(username=username_spec,
                                                                   video_ids_list=video_ids_mustafasarigul)

save_videolinks_to_csv(video_links_list=video_links_mustafasarigul_profile,
                       username=username)


#######10- SCRAPPING FOR @genel_secim_2023 - DONE
username = "genel_secim_2023"
genelsecim2023_dataframe_scrapped = auto_data_file_importer(username=username)

video_ids_genelsecim2023 = extract_video_ids(dataframe=genelsecim2023_dataframe_scrapped)

username_spec = "@genel_secim_2023"
video_links_genelsecim2023_profile = tiktok_link_generator_profile(username=username_spec,
                                                                   video_ids_list=video_ids_genelsecim2023)

save_videolinks_to_csv(video_links_list=video_links_genelsecim2023_profile,
                       username=username)


#######11- SCRAPPING FOR @secimhatti - DONE
username = "secimhatti"
secimhatti_dataframe_scrapped = auto_data_file_importer(username=username)

video_ids_secimhatti = extract_video_ids(dataframe=secimhatti_dataframe_scrapped)

username_spec = "@secimhatti"
video_links_secimhatti_profile = tiktok_link_generator_profile(username=username_spec,
                                                               video_ids_list=video_ids_secimhatti)

save_videolinks_to_csv(video_links_list=video_links_secimhatti_profile,
                       username=username)
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
