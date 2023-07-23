########################################################################################################################
########################################################################################################################
########################################################################################################################
######################################## ALL THE LAYERS OF DATA COLLECTION PROCESS #####################################


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




#FUNCTION 5:  #AUTOMATIC NDJSON CONVERTER TO CSV AND IMPORT INTO PYTHON ENV
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


#FUNCTION 6: #AUTOMATIC USERNAME AND VIDEO ID DETECTION FUNCTION FOR THE PAIRED DATA
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


#FUNCTION 7:  #AUTOMATIC TIKTOK LINK CREATION FUCNTION FOR HASHTAG SCRAPPING
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


#FUNCTION 8:  #AUTOMATIC CSV FILE CREATION FUNCTION
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




#FUNCTION 9:  #FINAL DATA FILE CREATOR FOR ALL THE LINKS
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


#FUNCTION 10:  #FINAL DATA FILE CREATOR FOR THE PROFILE LINKS
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

#FUNCTION 11: #FINAL DATA FILE CREATOR FOR THE HASHTAG LINKS
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


#FUNCTION 12:  #FINAL DATA FILE CREATOR FOR MORE DETAILED TYPE(DIVIDED VERSION)
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





#FUNCTION 13: #FUNCTION TO CREATE THE VALID TIKTOK LINK VERSION FOR PYTOK
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



#FUNCTION 14:  #FUNCTION TO CAPTURE THE LINKS FROM EACH COLUMN OF THE DETALIED LINK PRESENTATION DATA
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



#FUNCTION 15:  #FUNCTION TO DELETE THE DUPLICATE METADATA REGISTRATION BY PYTOK
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



#FUNCTION 16: #FUNCTION TO CREATE VIDEO PATHS IN THE DIRECTORY BY CONSIDERING THE MANIPULATED VIDEO LINKS
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




#FUNCTION 17:  #MORE AUTOMIZED VERSION OF TIKTOK WEBSCRAPPER - FUNCTION THAT AUTOMATICALLY REPORTS AND SCRAPS THE VIDEO
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


#FUNCTION 18: EXECUTION FUNCTION THAT CONSIDERS THE ABOVE SUPPLEMENTARY FUNCTIONS -  EXECUTES THE WHOLE SCRAPPING
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



#FUNCTION 19: #FUNCTION TO COMBINE ALL THE METADATA FORM THE EACH HASHTAG AND USER PROFILE
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



# Import required libraries
import time
import hashlib
import csv
import os
from selenium import webdriver
from selenium.webdriver.common.by import By

# Read list of video URLs
with open("video_list.txt", "r") as f:
    video_list = f.readlines()
    video_list = [video.strip() for video in video_list]
    print(video_list)

# Setup Selenium WebDriver
driver = webdriver.Firefox()
first_time = True

# Create directory for storing comments
if not os.path.exists("comments"):
    os.makedirs("comments")

# Begin loop through each video URL
for video in video_list:
    # Create unique filename for each video's comments
    video_filename = video.replace('/', '_').replace(':', '_') + ".csv"

    # Check if comments for this video have already been parsed
    if os.path.exists(os.path.join("comments", video_filename)):
        print(f"Already parsed comments for {video}, skipping")
        continue

    # Navigate to video page in browser
    old_hash = ""
    driver.get(video)
    # Pause for user interaction on first iteration
    if first_time:
        input("Press Enter to continue...")
        first_time = False

    # Begin loop to scrape comments from video page
    while True:
        # Find comment elements on page
        comment_elements = driver.find_elements(By.CSS_SELECTOR,'p[data-e2e="comment-level-1"]')

        # Extract comment text
        comments = [element.text for element in comment_elements]

        # Check for new comments using hash comparison
        comments_concatenated = "".join(comments)
        sha256_hash = hashlib.sha256(comments_concatenated.encode()).hexdigest()
        if sha256_hash == old_hash:
            print("No new comments")
            break
        old_hash = sha256_hash

        # Scroll page to load more comments and pause to allow loading
        for comment in comments:
            print(comment)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(4)

    # Write comments to file
    with open(os.path.join("comments", video_filename), "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        for comment in comments:
            writer.writerow([comment])

# Close browser after all videos have been processed
driver.quit()
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
