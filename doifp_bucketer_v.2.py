import re
import pyodbc
import string
from collections import Counter
import collections
import dictionary

#THIS IS WHERE YOU EDIT THE BUCKETS AND THE DICTIONARY TERMS IN CONNECTION WITH THEM. 

keyword_dictionary = {
    'Animals' : {'animal', 'dog', 'cat'},
    'Party' : {'party', 'confetti','wall'},
    'People' : {'female','male''happy','people','people group','people ','crowd','women','men','attractive','adults','smiling','group','attractive','two','three','four','five','six','seven',
                'eight','nine','ten','2','3','4','5','6','7','8','9','10','costumes','happy','male','female','drunkard','caucasion','adults','adults','crowd','group','woman','man'},
    'Art' : {'art', 'sculpture', 'fearns', 'pumpkin','design','graphic','logo','black','light','lantern','painting','graffiti','graffito'},
    'Buildings' : {'building', 'architecture', 'gothic', 'skyscraper'},
    'Vehicle' : {'car','formula','f-1','f1','f 1','f one','f-one','moped','mo ped','mo-ped','scooter','limousine'},
    'Person' : {'person','dress','shirt','woman','man','attractive','adult','smiling','sleeveless','halter','spectacles','button','bodycon', 'face','costume'},
    'Food' : {'food','plate','chicken','steak','pizza','pasta','meal','asian','beef','cake','candy','food pyramid','spaghetti','curry','lamb','sushi','meatballs','biscuit',
              'apples','meat','mushroom','jelly', 'sorbet','nacho','burrito','taco','cheese','chocolate'},
    'Glasses' : {'glass','drink','container','glasses','cup','clear','alcohol','glasses'},
    'Bottles' : {'bottle','whiskey','whisky','bottles','wine','bottles'},
    'Signage' : {'sign','ad','advert','card','logo','mat','logos','martini','bacardi','signage','signs'},
    'Slogan' : {'grand journey'},
    'DJ' : {'dj','disc','jockey','mixer','instrument','turntable'},
    'Nature' : {'tree','plant','leaf','flower','roses','poppys','clouds'}
}

########################################################################################

cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=ENTER SERVER NAME;DATABASE=ENTER DATABASE NAME;UID=ENTER USER ID;PWD=ENTER PASSWORD')
table_name = 'ENTER TABLE NAME'
cursor = cnxn.cursor()
cursor.execute("SELECT [unique_id] \
      ,[thedate] \
      ,[url] \
      ,[cloudsight_result] \
      ,[imagga_result_1] \
      ,[imagga_result_2] \
      ,[country_code] \
      ,[google_logo_recognition] \
      ,[google_text_recognition] \
      ,[bucket] \
       FROM [BRPMEN].[dbo].[ENTER TABLE NAME] \
       WHERE [project_name] LIKE '%ENTER PROJECT NAME%'") #THIS IS WHERE YOU EDIT THE PROJECT NAME #IF YOU ARE DOING THE BOMBAY Q4 2016 PROJECT, CHANGE THIS TO WHATEVER YOU CALLED THE PROJECT. MOST LIKELY: '%bombay_q4_2016_report%'

rows = cursor.fetchall()

for row in rows:
    url = row[2]
    url_gapless = url.replace("\n","")
    cloudsight = row[3]
    imagga_1 = row[4]
    imagga_2 = row[5]
    country = row[6]
    bucket = row[9]

    #LINES BELOW COMBINES THE DATA, CLEANS THE DATA, FINDS THE MOST COMMON WORD IN THE DATA. DELETES PUNCUATION. DELETES NUMBERS.
    
    image_results_combined = str(cloudsight), str(imagga_2), str(imagga_2)
    image_results_nopunc = str(image_results_combined).translate(None, string.punctuation)
    image_results_nonums = ''.join([i for i in image_results_nopunc if not i.isdigit()])
    image_results_count = collections.Counter(image_results_nonums.split())
    image_results_most_common = image_results_count.most_common()
    
    image_results_highest = image_results_most_common[0]

    image_content = image_results_highest[0]
    num_of_image_opinions = image_results_highest[1]

    #LINES BELOW IGNORE ANY FIELDS THAT ARE CURRENTLY NONE, DROPS DATA INTO DATABASE IF BUCKET HAS MORE THAN ONE OPINION.
    #IF OPINIONS FOR IMAGE ARE UNIQUE THEN IT TAKES THE FIRST WORD FROM CLOUDSIGHT AND FINDS THE BUCKET FOR IT.

    if "None" in str(bucket): #IF NONE IN BUCKET, THEN CONTINUE
        if "None" in str(image_content): #IF NONE IN FIRST SENTENCE, THEN CONTINUE
            
            image_content_first_word = image_results_nonums.partition(' ')[0] #PULL FIRST WORD

            if "None" in str(image_content_first_word): #IF NONE IN FIRST WORD, THEN PASS
                pass
            
            if "None" not in str(image_content_first_word): #IF NONE NOT IN FIRST WORD, THEN CONTINUE 
                for key, value in keyword_dictionary.iteritems():
                    if image_content_first_word in value: #IF FIRST WORD IN DICTIONARY, THEN STORE IN DATABASE
                        print key,",",url,",",",",country

                        cnxn2 = pyodbc.connect('DRIVER={SQL Server};SERVER=ENTER SERVER NAME;DATABASE=ENTER DATABASE NAME;UID=ENTER USER ID;PWD=ENTER PASSWORD')
                        cursor2 = cnxn2.cursor()
                        table_name = 'ENTER TABLE NAME'
                        cursor2.execute("UPDATE " +table_name+ " SET bucket=? WHERE url=?",key,url)
                        cnxn2.commit()
                        cnxn2.close()
                    
                    if image_content_first_word not in value:
                        pass
                
        if "None" not in str(image_content): #IF NONE NOT IN FIRST SENTENCE, THEN CONTINUE
            
            if num_of_image_opinions > 1: #IF NUM IS GREATER THAN 1, THEN CONTINUE

                for key, value in keyword_dictionary.iteritems() :
                    
                    if image_content in value: #IF OUR RESULT IN THE DICTIONARY, STORE IN DATABASE
                        print key,",",url,",",country

                        cursor = cnxn.cursor()
                        cursor.execute("UPDATE " +table_name+ " SET bucket=? WHERE url=?",key,url)
                        cnxn.commit()
                        cnxn.close()
                        
                    if image_content not in value: #IF OUR RESULT NOT IN DICTIONARY, THEN PASS
                       #print image_content
                        pass
                    
            if num_of_image_opinions < 1:
                pass

    if "None" not in str(bucket): #IF NONE NOT IN BUCKET, THEN PASS 
        pass



