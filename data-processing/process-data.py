from neo4j.v1 import GraphDatabase
import glob
import os
import re
import nltk
from nltk.corpus import wordnet as wn
from nltk.tokenize import sent_tokenize 
from nltk.tokenize import word_tokenize

# from nltk.corpus import stopwords
# stopwords.words('english')

nltk.download('punkt')
nltk.download('wordnet')


# Setup neo4j driver once
neo4j_args = {
    "uri" : "bolt://localhost:7688",
    "user" : "neo4j",
    "password" : "bauhaus"
}
neo4j_driver = GraphDatabase.driver(
    neo4j_args['uri'], auth = (neo4j_args['user'],neo4j_args['password'])
)

# load files
data_dir = 'data'

output_dir = "output"

for filename in glob.glob(data_dir + '/*.txt'):

    book_title = filename.replace(data_dir + "/", "").split("_")[1].split(".")[0]

    final_sentences = []
    with open(filename, 'r', encoding='utf-8') as file:
        file_content = file.read()
        file_content = re.sub('\s+', ' ', file_content).strip()
        sentences = sent_tokenize(file_content)
        
        for sentence in sentences:
            words = word_tokenize(sentence)
            has_verb = False

            if len(words) > 12 and len([x for x in ["INDEX", "APPENDIX"] if x in words]) == 0:
                for w in words:
                    pos_l = set()
                    for tmp in wn.synsets(w):
                        if tmp.pos() == 'v':
                            has_verb = True
                            break
            if has_verb:
                final_sentences.append(sentence)
    file.close()


    if not os.path.exists(output_dir + "/" + book_title):
        os.makedirs(output_dir + "/" + book_title)

    with open(output_dir + "/" + book_title + "/sentences.txt", 'w+', encoding='utf-8') as file_sentences:
        for sentence in final_sentences:
            file_sentences.write(sentence + "\n")
    file_sentences.close()

        # file_lines = file.readline()
        # total_lines = len(file_lines)

        # # Parse
        # book_author = filename.replace(data_dir + "/", "").split("_")[0].replace("-"," ").title()

        # # find pages, everything before a page is the content of that page
        # # pages = file_content.

        # page_texts = {}
        # page_text = ""

        # c = 1
        # for line in file_lines:
        #   line = line.replace("\n", " ").strip()

        #   check_integer = False
        #   try:
        #       check_integer = int(line)
        #   except:
        #       pass

        #   if check_integer:

        #       # add the page's text
        #       page_texts[line] = page_text

        #       # reset
        #       page_text = ""

        #   elif(c != total_lines):
        #       page_text += line

        #   c += 1


        # for k,v in page_texts.items():
        #   print(k+"test")
        #   print(v)
        #   print("\n\n")




# Close neo4j driver
neo4j_driver.close()