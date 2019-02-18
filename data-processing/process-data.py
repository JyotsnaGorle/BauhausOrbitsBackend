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

def add_book_sentence(tx, book, sentence):
    tx.run(
    "MERGE (b:Book {book_id: $b_id}) " +
    "SET b.author = $b_author, b.title = $b_title " + 
    "MERGE (s:Sentence {sentence_id: $s_id}) " + 
    "SET s.text = $s_text, s.page = $s_page, s.sentiment_score = $s_sentiment_score " +
    "MERGE (s)-[:IS_IN]->(b)", b_id=book['id'], b_author=book["author"], b_title=book["title"], s_id=sentence['id'], s_text=sentence['text'], s_page=sentence["page"], s_sentiment_score=sentence['sentiment_score'])

def run():
    # Setup neo4j driver once
    neo4j_args = {
        "uri" : "bolt://localhost:7688",
        "user" : "neo4j",
        "password" : "bauhaus"
    }
    neo4j_driver = GraphDatabase.driver(
        neo4j_args['uri'], auth = (neo4j_args['user'],neo4j_args['password'])
    )

    with neo4j_driver.session() as session:

        # Book "id" field is a unique key
        c_p = session.run("CREATE CONSTRAINT ON (b:Book) ASSERT b.book_id IS UNIQUE")
        
        # Sentence "ids" field is a unique key
        c_a = session.run("CREATE CONSTRAINT ON (s:Sentence) ASSERT s.sentence_id IS UNIQUE")

        # load files
        data_dir = "data"
        output_dir = "output"

        book_counter = 0

        for subdir, book_dir, book_files in os.walk(data_dir):

            if not book_dir:
                continue

            book_counter += 1

            book_meta = book_dir[0].split("_")
            book_author = book_meta[0].replace("-", " ")
            book_title = book_meta[1]

            print(book_title)

            book = {
                "id": book_counter,
                "title": book_title,
                "author": book_author
            }

            # Save book
            # session.write_transaction(add_book, book)
            sentence_counter = 1

            for filename in glob.glob(data_dir + '/' + book_dir[0] + '/*.txt'):

                current_page = filename.split(" ")[-1].replace(".pdf.txt", "").replace(book_title, "")
                print("......... Page: " + str(current_page))

                final_sentences = []
                with open(filename, 'r', encoding="latin-1") as file:
                    file_content = file.read()
                    file_content = re.sub('\s+', ' ', file_content).strip()
                    sentences = sent_tokenize(file_content)
                    
                    total_sentences = len(sentences)

                    c = 0
                    for sentence in sentences:
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

                            sentence_dict = {
                                "id": sentence_counter,
                                "text": sentence,
                                "sentiment_score": sentiment_score,
                                "page": current_page
                            }

                            # Save sentence
                            session.write_transaction(add_book_sentence, book, sentence_dict)

                            sentence_counter += 1

                        c +=1
                print("................. " + str(sentence_counter) + " sentences added")
                file.close()

                # Write output
                # output_path = output_dir + "/" + book_dir[0]
                # print("Writing output to: " + str(output_path))

                # if not os.path.exists(output_path):
                #     os.makedirs(output_path)
                # with open(output_path + "/" + str(current_page) + ".txt", 'w+', encoding='utf-8') as file_sentences:
                #     for row in final_sentences:
                #         file_sentences.write(row["sentiment"] + "," + row["sentence"] + "\n")
                # file_sentences.close()

    # Close neo4j driver
    neo4j_driver.close()

run()