from neo4j.v1 import GraphDatabase
import glob
import os
import re
import nltk
from nltk.corpus import wordnet as wn
from nltk.tokenize import sent_tokenize 
from nltk.tokenize import word_tokenize
from textblob import TextBlob

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
        
        print("Processing sentences...")
        total_sentences = len(sentences)
        counter = 0

        c = 0
        for sentence in sentences:
            print(str(c) + "/" + str(total_sentences))
            words = word_tokenize(sentence)
            has_verb = False

            # Wword count cutoff
            if len(words) > 12 and len([x for x in ["INDEX", "APPENDIX"] if x in words]) == 0:
                for w in words:
                    pos_l = set()
                    for tmp in wn.synsets(w):
                        # At least a verb in the sentence
                        if tmp.pos() == 'v':
                            has_verb = True
                            break
            if has_verb:
                
                # Sentiment Analysis
                sentiment_score = round(TextBlob(sentence).sentiment.polarity, 3)
                sentiment_label = 'positive' if sentiment_score > 0 \
                                             else 'negative' if sentiment_score < 0 \
                                                 else 'neutral'
                final_sentences.append({
                    "sentiment": sentiment_label,
                    "sentence": sentence
                })

                counter += 1

            c +=1
    file.close()

    # Write output
    output_path = output_dir + "/" + book_title + "/sentences.txt"
    print("Writing output to: " + str(output_path))

    if not os.path.exists(output_dir + "/" + book_title):
        os.makedirs(output_dir + "/" + book_title)
    with open(output_path, 'w+', encoding='utf-8') as file_sentences:
        for row in final_sentences:
            file_sentences.write(row["sentiment"] + "," + row["sentence"] + "\n")
    file_sentences.close()

# Close neo4j driver
neo4j_driver.close()