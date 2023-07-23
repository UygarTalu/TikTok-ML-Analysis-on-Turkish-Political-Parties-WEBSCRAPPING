########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

######################################## WEBSCRAPPING FOR HASHTAGS - 2 #################################################

###INFORMATION ABOUT THE SCRIPT(WEB SCRAPPING - 2)

"""
IN THE ANALYSIS LAYER WE HAVE 4 DIFFERENT FUNCTIONS.
1- "auto_data_file_importer_hashtag"
2- "extract_usernames_and_ids_hashtag"
3- "tiktok_link_generator_hashtag_generalized"
4- "save_videolinks_to_csv_hashtag"

Results for Web Scrapping 2- In this layer of analysis, we first use Zeeschuimer to scrap the data objects form user
profiles and hashtags and then we capture the unique video ids from the downloaded ndjson files. Depending on the unique
video ids we created the TikTok link structure for each video. In the resulting dataframe we have labels as hashtag
videos and user profiles. In the rows we have related video links.

Label: Hashtag or user profile label for the processed video.
"""
########################################################################################################################
########################################################################################################################


#FUNCTION 1:  #AUTOMATIC NDJSON CONVERTER TO CSV AND IMPORT INTO PYTHON ENV
########################################################################################################################

#Imports a data file in ndjson format for a given hashtag name.
#Converts the imported data into a pandas DataFrame.
#Saves the DataFrame as a CSV file.
#Useful for importing and preprocessing data files associated with specific hashtags for further analysis.


def auto_data_file_importer_hashtag(hashtag_name):
    """
    Imports the data file in ndjson format for a given hashtag name and converts it to a CSV file.

    Args:
        hashtag_name (str): The hashtag name for which the data file will be imported.

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

    ndjson_file_path_local = default_ndjson_path_1 + hashtag_name + default_path_addition + default_ndjson_path_format_1
    csv_file_path_local = default_ndjson_path_1 + hashtag_name + default_path_addition + default_ndjson_path_format_2

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


#FUNCTION 2: #AUTOMATIC USERNAME AND VIDEO ID DETECTION FUNCTION FOR THE PAIRED DATA
########################################################################################################################

#Takes a DataFrame and a hashtag name as input.
#Extracts the usernames and video IDs from the DataFrame for the given hashtag.
#Counts the occurrences of each username to understand if same users are posting multiple videos in the same hashtag.
#Returns a tuple containing a list of username and video ID pairs and a dictionary with username occurrences.
#Useful for extracting relevant information from a DataFrame related to a specific hashtag and analyzing username patterns.

def extract_usernames_and_ids_hashtag(dataframe, hashtag_name):
    """
    Extracts the usernames and video IDs from a dataframe for a given hashtag name.

    Args:
        dataframe (pd.DataFrame): The dataframe containing the 'data' column.
        hashtag_name (str): The hashtag name associated with the dataframe.

    Returns:
        tuple: A tuple containing a list of username and video ID pairs, and a dictionary with username occurrences.

    """
    import ast

    user_id_pairs = []
    username_counter = {}
    for i, row in enumerate(dataframe['data']):
        try:
            row_dict = ast.literal_eval(row)
            author = row_dict.get('author', None)
            video_id = row_dict.get('id', None)
            if isinstance(author, dict):
                username = author.get('uniqueId', None)
            else:
                username = author

            if username is None:
                print(f"Row {i}: Missing username")
            if video_id is None:
                print(f"Row {i}: Missing video id")

            if username is not None and video_id is not None:
                user_id_pairs.append((username, video_id))

                # Updating the count of username occurrences
                if username in username_counter:
                    username_counter[username] += 1
                else:
                    username_counter[username] = 1

        except ValueError as e:
            print(f'Row {i}: ValueError when parsing row. Error: {e}')
            continue

    # Count usernames that occurred multiple times
    multiple_occurrences = sum(1 for count in username_counter.values() if count > 1)

    # Count total number of unique users
    total_unique_users = len(username_counter)

    print(
        f"For the {hashtag_name} hashtag, video ids and usernames are captured. The number of multiple times occurred usernames is: {multiple_occurrences}")
    print(f"The total number of unique users is: {total_unique_users}")
    print("The total number of together captured video ids and usernames is", len(user_id_pairs))

    return user_id_pairs, username_counter

########################################################################################################################
########################################################################################################################


#FUNCTION 3:  #AUTOMATIC TIKTOK LINK CREATION FUCNTION FOR HASHTAG SCRAPPING
########################################################################################################################

#Generates TikTok video links for a given list of user ID and video ID pairs.
#Iterates over each pair and constructs the TikTok video link using the base URL, username with "@" sign, video ID, and additional parameters.
#Appends the generated link to a list.
#Prints the number of generated links and the number of captured user ID and video ID pairs.
#Returns the list of generated TikTok video links.

def tiktok_link_generator_hashtag_generalized(userid_videoid_pairs):
    """
    Generates TikTok video links for a given list of user ID and video ID pairs.

    Args:
        userid_videoid_pairs (list): A list of tuples containing user ID and video ID pairs.

    Returns:
        list: A list of generated TikTok video links.
    """
    ready_links = []

    for username, video_id in userid_videoid_pairs:
        ready_username = "@" + username

        base_url = "https://www.tiktok.com/"
        rest_link = "/video/"
        last_sign = "?"

        link_generated = base_url + ready_username + rest_link + video_id + last_sign

        ready_links.append(link_generated)

    return ready_links

########################################################################################################################
########################################################################################################################


#FUNCTION 3:  #AUTOMATIC CSV FILE CREATION FUNCTION
########################################################################################################################

#Saves a list of video links to a CSV file.
#Converts the video links list into a pandas DataFrame.
#Constructs the file name for the CSV file using the hashtag name and additional information.
#Specifies the file path for the CSV file on the desktop.
#Saves the DataFrame as a CSV file.
#Prints the file path where the CSV file is saved.

def save_videolinks_to_csv_hashtag(video_links_list, hashtag_name):
    """
    Saves a list of video links to a CSV file.

    Args:
        video_links_list (list): A list of video links.
        hashtag_name (str): The hashtag name used for generating the file name.

    Returns:
        None
    """
    import pandas as pd
    import os

    addition_1 = "_SCRAPPED"
    addition_2 = "_VIDEO_LINKS_HASHTAG"

    file_name = hashtag_name + addition_1 + addition_2

    desktop_path = os.path.expanduser("~/Desktop")
    csv_file_path = os.path.join(desktop_path, file_name + ".csv")

    video_links_df = pd.DataFrame(video_links_list)

    video_links_df.to_csv(csv_file_path, index=False)
    print(f"DataFrame {file_name} saved as CSV: {csv_file_path}")

########################################################################################################################
########################################################################################################################
########################################################################################################################


#### EXECUTION OF THE FUNCTIONS
########################################################################################################################
########################################################################################################################
############################### EXECUTION FOR HASHTAG VIDEOS ###########################################################

####### SCRAPPING FOR ---Orginal name of the hashtag is #secim --1 -- Ready - Done
hashtag_name = "secim"
secim_hastag_dataframe_scrapped = auto_data_file_importer_hashtag(hashtag_name=hashtag_name)

user_id_pairs_secim_hashtag, username_counter_tracker_secim_hashtag = extract_usernames_and_ids_hashtag(dataframe= secim_hastag_dataframe_scrapped,
                                                                                                        hashtag_name=hashtag_name)

video_links_secim_hashtag = tiktok_link_generator_hashtag_generalized(userid_videoid_pairs=user_id_pairs_secim_hashtag)

save_videolinks_to_csv_hashtag(video_links_list=video_links_secim_hashtag,
                               hashtag_name=hashtag_name)

####### SCRAPPING FOR ---Orginal name of the hashtag is #secim2023 --2 -- Ready - Done
hashtag_name = "secim2023"
secim2023_hastag_dataframe_scrapped = auto_data_file_importer_hashtag(hashtag_name=hashtag_name)

user_id_pairs_secim2023_hashtag, username_counter_tracker_secim2023_hashtag = extract_usernames_and_ids_hashtag(dataframe= secim2023_hastag_dataframe_scrapped,
                                                                                                                hashtag_name=hashtag_name)

video_links_secim2023_hashtag = tiktok_link_generator_hashtag_generalized(userid_videoid_pairs=user_id_pairs_secim2023_hashtag)


save_videolinks_to_csv_hashtag(video_links_list=video_links_secim2023_hashtag,
                               hashtag_name=hashtag_name)

####### SCRAPPING FOR ---Orginal name of the hashtag is #herseyçokgüzelolucak --3 -- Ready - Done
hashtag_name = "herşeyçokgüzelolacak"
herseycokguzelolucak_hastag_dataframe_scrapped = auto_data_file_importer_hashtag(hashtag_name=hashtag_name)

user_id_pairs_herseycokguzelolucak_hashtag, username_counter_tracker_herseycokguzelolucak_hashtag = extract_usernames_and_ids_hashtag(dataframe= herseycokguzelolucak_hastag_dataframe_scrapped,
                                                                                                                                      hashtag_name=hashtag_name)

video_links_herseycokguzelolucak_hashtag = tiktok_link_generator_hashtag_generalized(userid_videoid_pairs=user_id_pairs_herseycokguzelolucak_hashtag)


save_videolinks_to_csv_hashtag(video_links_list=video_links_herseycokguzelolucak_hashtag,
                               hashtag_name=hashtag_name)

####### SCRAPPING FOR ---Orginal name of the hashtag is #benkemalgeliyorum --4 -- Ready -Done
hashtag_name = "benkemalgeliyorum"
benkemalgeliyorum_hastag_dataframe_scrapped = auto_data_file_importer_hashtag(hashtag_name=hashtag_name)


user_id_pairs_benkemalgeliyorum_hashtag, username_counter_tracker_benkemalgeliyorum_hashtag = extract_usernames_and_ids_hashtag(dataframe= benkemalgeliyorum_hastag_dataframe_scrapped,
                                                                                                                                hashtag_name=hashtag_name)

video_links_benkemalgeliyorum_hashtag = tiktok_link_generator_hashtag_generalized(userid_videoid_pairs=user_id_pairs_benkemalgeliyorum_hashtag)


save_videolinks_to_csv_hashtag(video_links_list=video_links_benkemalgeliyorum_hashtag,
                               hashtag_name=hashtag_name)

####### SCRAPPING FOR ---Orginal name of the hashtag is #sanasöz --5 -- Ready -Done
hashtag_name = "sanasöz"
sanasoz_hastag_dataframe_scrapped = auto_data_file_importer_hashtag(hashtag_name=hashtag_name)


user_id_pairs_sanasoz_hashtag, username_counter_tracker_sanasoz_hashtag = extract_usernames_and_ids_hashtag(dataframe= sanasoz_hastag_dataframe_scrapped,
                                                                                                            hashtag_name=hashtag_name)

video_links_sanasoz_hashtag = tiktok_link_generator_hashtag_generalized(userid_videoid_pairs=user_id_pairs_sanasoz_hashtag)


save_videolinks_to_csv_hashtag(video_links_list=video_links_sanasoz_hashtag,
                               hashtag_name=hashtag_name)

####### SCRAPPING FOR ---Orginal name of the hashtag is #dogruadam --6 - Ready -Done
hashtag_name = "dogruadam"
dogruadam_hastag_dataframe_scrapped = auto_data_file_importer_hashtag(hashtag_name=hashtag_name)


user_id_pairs_dogruadam_hashtag, username_counter_tracker_dogruadam_hashtag = extract_usernames_and_ids_hashtag(dataframe= dogruadam_hastag_dataframe_scrapped,
                                                                                                                hashtag_name=hashtag_name)

video_links_dogruadam_hashtag = tiktok_link_generator_hashtag_generalized(userid_videoid_pairs=user_id_pairs_dogruadam_hashtag)


save_videolinks_to_csv_hashtag(video_links_list=video_links_dogruadam_hashtag,
                               hashtag_name=hashtag_name)

####### SCRAPPING FOR ---Orginal name of the hashtag is #türkiyeyüzyılı --7 - Ready -Done
hashtag_name = "türkiyeyüzyılı"
turkiyeyuzyili_hastag_dataframe_scrapped = auto_data_file_importer_hashtag(hashtag_name=hashtag_name)


user_id_pairs_turkiyeyuzyili_hashtag, username_counter_tracker_turkiyeyuzyili_hashtag = extract_usernames_and_ids_hashtag(dataframe= turkiyeyuzyili_hastag_dataframe_scrapped,
                                                                                                                          hashtag_name=hashtag_name)

video_links_turkiyeyuzyili_hashtag = tiktok_link_generator_hashtag_generalized(userid_videoid_pairs=user_id_pairs_turkiyeyuzyili_hashtag)


save_videolinks_to_csv_hashtag(video_links_list=video_links_turkiyeyuzyili_hashtag,
                               hashtag_name=hashtag_name)

####### SCRAPPING FOR ---Orginal name of the hashtag is #hedef2023 --8 -- Ready -Done
hashtag_name = "hedef2023"
hedef2023_hastag_dataframe_scrapped = auto_data_file_importer_hashtag(hashtag_name=hashtag_name)


user_id_pairs_hedef2023_hashtag, username_counter_tracker_hedef2023_hashtag = extract_usernames_and_ids_hashtag(dataframe= hedef2023_hastag_dataframe_scrapped,
                                                                                                                hashtag_name=hashtag_name)

video_links_hedef2023_hashtag = tiktok_link_generator_hashtag_generalized(userid_videoid_pairs=user_id_pairs_hedef2023_hashtag)


save_videolinks_to_csv_hashtag(video_links_list=video_links_hedef2023_hashtag,
                               hashtag_name=hashtag_name)

####### SCRAPPING FOR ---Orginal name of the hashtag is #14mayıs --9 - Ready -Done
hashtag_name = "14mayıs"
ondortmayis_hastag_dataframe_scrapped = auto_data_file_importer_hashtag(hashtag_name=hashtag_name)


user_id_pairs_ondortmayis_hashtag, username_counter_tracker_hedef2023_hashtag = extract_usernames_and_ids_hashtag(dataframe= ondortmayis_hastag_dataframe_scrapped,
                                                                                                                  hashtag_name=hashtag_name)

video_links_ondortmayis_hashtag = tiktok_link_generator_hashtag_generalized(userid_videoid_pairs=user_id_pairs_ondortmayis_hashtag)


save_videolinks_to_csv_hashtag(video_links_list=video_links_ondortmayis_hashtag,
                               hashtag_name=hashtag_name)

####### SCRAPPING FOR ---Orginal name of the hashtag is #28mayıs --10 --Ready -Done
hashtag_name = "28mayıs"
yirmisekizmayis_hastag_dataframe_scrapped = auto_data_file_importer_hashtag(hashtag_name=hashtag_name)


user_id_pairs_yirmisekizmayis_hashtag, username_counter_tracker_yirmisekizmayis_hashtag = extract_usernames_and_ids_hashtag(dataframe= yirmisekizmayis_hastag_dataframe_scrapped,
                                                                                                                            hashtag_name=hashtag_name)

video_links_yirmisekizmayis_hashtag = tiktok_link_generator_hashtag_generalized(userid_videoid_pairs=user_id_pairs_yirmisekizmayis_hashtag)


save_videolinks_to_csv_hashtag(video_links_list=video_links_yirmisekizmayis_hashtag,
                               hashtag_name=hashtag_name)

####### SCRAPPING FOR ---Orginal name of the hashtag is #mevzularaçıkmikrofon --11 -Ready -Done
hashtag_name = "mevzularaçıkmikrofon"
mevzularacikmikrofon_hastag_dataframe_scrapped = auto_data_file_importer_hashtag(hashtag_name=hashtag_name)


user_id_pairs_mevzularacikmikrofon_hashtag, username_counter_tracker_yirmisekizmayis_hashtag = extract_usernames_and_ids_hashtag(dataframe= mevzularacikmikrofon_hastag_dataframe_scrapped,
                                                                                                                                 hashtag_name=hashtag_name)

video_links_mevzularacikmikrofon_hashtag = tiktok_link_generator_hashtag_generalized(userid_videoid_pairs=user_id_pairs_mevzularacikmikrofon_hashtag)


save_videolinks_to_csv_hashtag(video_links_list=video_links_mevzularacikmikrofon_hashtag,
                               hashtag_name=hashtag_name)

####### SCRAPPING FOR ---Orginal name of the hashtag is #reis --12 --Ready -Done
hashtag_name = "reis"
reis_hastag_dataframe_scrapped = auto_data_file_importer_hashtag(hashtag_name=hashtag_name)


user_id_pairs_reis_hashtag, username_counter_tracker_reis_hashtag = extract_usernames_and_ids_hashtag(dataframe= reis_hastag_dataframe_scrapped,
                                                                                                      hashtag_name=hashtag_name)

video_links_reis_hashtag = tiktok_link_generator_hashtag_generalized(userid_videoid_pairs=user_id_pairs_reis_hashtag)


save_videolinks_to_csv_hashtag(video_links_list=video_links_reis_hashtag,
                               hashtag_name=hashtag_name)

####### SCRAPPING FOR ---Orginal name of the hashtag is #akp --13 --Ready -Done
hashtag_name = "akp"
akp_hastag_dataframe_scrapped = auto_data_file_importer_hashtag(hashtag_name=hashtag_name)


user_id_pairs_akp_hashtag, username_counter_tracker_reis_hashtag = extract_usernames_and_ids_hashtag(dataframe= akp_hastag_dataframe_scrapped,
                                                                                                     hashtag_name=hashtag_name)

video_links_akp_hashtag = tiktok_link_generator_hashtag_generalized(userid_videoid_pairs=user_id_pairs_akp_hashtag)


save_videolinks_to_csv_hashtag(video_links_list=video_links_akp_hashtag,
                               hashtag_name=hashtag_name)

####### SCRAPPING FOR ---Orginal name of the hashtag is #akparti --14 --Ready -Done
hashtag_name = "akparti"
akparti_hastag_dataframe_scrapped = auto_data_file_importer_hashtag(hashtag_name=hashtag_name)


user_id_pairs_akparti_hashtag, username_counter_tracker_akparti_hashtag = extract_usernames_and_ids_hashtag(dataframe= akparti_hastag_dataframe_scrapped,
                                                                                                         hashtag_name=hashtag_name)

video_links_akparti_hashtag = tiktok_link_generator_hashtag_generalized(userid_videoid_pairs=user_id_pairs_akparti_hashtag)


save_videolinks_to_csv_hashtag(video_links_list=video_links_akparti_hashtag,
                               hashtag_name=hashtag_name)

####### SCRAPPING FOR ---Orginal name of the hashtag is #chp --15 --Ready -Done
hashtag_name = "chp"
chp_hastag_dataframe_scrapped = auto_data_file_importer_hashtag(hashtag_name=hashtag_name)


user_id_pairs_chp_hashtag, username_counter_tracker_chp_hashtag = extract_usernames_and_ids_hashtag(dataframe= chp_hastag_dataframe_scrapped,
                                                                                                        hashtag_name=hashtag_name)

video_links_chp_hashtag = tiktok_link_generator_hashtag_generalized(userid_videoid_pairs=user_id_pairs_chp_hashtag)


save_videolinks_to_csv_hashtag(video_links_list=video_links_chp_hashtag,
                               hashtag_name=hashtag_name)

####### SCRAPPING FOR ---Orginal name of the hashtag is #milletittifakı --16 -Ready -Done
hashtag_name = "milletittifakı"
milletittifaki_hastag_dataframe_scrapped = auto_data_file_importer_hashtag(hashtag_name=hashtag_name)


user_id_pairs_milletittifaki_hashtag, username_counter_tracker_milletittifaki_hashtag = extract_usernames_and_ids_hashtag(dataframe= milletittifaki_hastag_dataframe_scrapped,
                                                                                                                          hashtag_name=hashtag_name)

video_links_milletittifaki_hashtag = tiktok_link_generator_hashtag_generalized(userid_videoid_pairs=user_id_pairs_milletittifaki_hashtag)


save_videolinks_to_csv_hashtag(video_links_list=video_links_milletittifaki_hashtag,
                               hashtag_name=hashtag_name)

####### SCRAPPING FOR ---Orginal name of the hashtag is #cumhurbaşkanı --17 -Ready -Done
hashtag_name = "cumhurbaskani"
cumhurbaskani_hastag_dataframe_scrapped = auto_data_file_importer_hashtag(hashtag_name=hashtag_name)


user_id_pairs_cumhurbaskani_hashtag, username_counter_tracker_cumhurbaskani_hashtag = extract_usernames_and_ids_hashtag(dataframe= cumhurbaskani_hastag_dataframe_scrapped,
                                                                                                                          hashtag_name=hashtag_name)

video_links_cumhurbaskani_hashtag = tiktok_link_generator_hashtag_generalized(userid_videoid_pairs=user_id_pairs_cumhurbaskani_hashtag)


save_videolinks_to_csv_hashtag(video_links_list=video_links_cumhurbaskani_hashtag,
                               hashtag_name=hashtag_name)
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################