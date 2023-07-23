########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

############################# PARSER FOR SCRAPING COMMENTS FOR THE SCRAPED VIDEOS  #####################################

###INFORMATION ABOUT THE SCRIPT(WEB SCRAPPING - 4)

### This layer of analysis contains 12 different sections to perform the comment scraping for the tiktok links
### that we generated in the previous layers of the webscraping analysis. Each step is numbered and explained below for
### clear understanding.


# 1 - This Python script is designed to scrape and store the comments from a list of specified TikTok videos.
#It achieves this task by following these steps:

# 2 - The script begins by reading a text file "video_list.txt" which contains a list of URLs of the TikTok videos from
#which the comments need to be scraped.

# 3 - A new Firefox browser instance is opened using Selenium WebDriver. For the first run, the script will pause and wait
#for the user to press Enter. This pause allows the user to manually login to their TikTok account if necessary.

# 4 - The script then checks if a directory named "comments" exists in the current directory. If not, it creates this
#directory. This directory will be used to store all the scraped comments.

# 5 - For each video URL from the list, the script generates a filename. It checks if a CSV file with this name exists in
#the "comments" directory. If the file exists, it means the comments for this video have been previously scraped,
#so it skips to the next video URL.

# 6 - If the file does not exist, the browser navigates to the video's URL.

# 7 - In a loop, the script finds all the comment elements on the page
#(each comment is contained within a <p> tag with a 'data-e2e' attribute value 'comment-level-1').

# 8 - The text for each comment is extracted and stored in a list.

# 9 - The script then computes a SHA256 hash of the concatenated comments. If this hash is the same as the hash computed
#in the previous loop iteration, it indicates that no new comments were loaded, and the script breaks out of the loop.

# 10 - Otherwise, the script scrolls to the bottom of the page to load more comments, waits for a few seconds to ensure
#comments are loaded, and repeats the process.

# 11 - After breaking out of the loop, the script writes the comments to a CSV file named after the video URL in
#the "comments" directory.

# 12 - Once all videos have been processed, the browser is closed.

#Note: This script depends on the page structure of TikTok's website as of the time of writing, so changes
#to TikTok's website may break the script. Also, Selenium WebDriver must be properly set up for this script to work
########################################################################################################################
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
