import glob
import os
import re
import csv
import nltk
from nltk.corpus import wordnet as wn
from nltk.tokenize import sent_tokenize 
from nltk.tokenize import word_tokenize
from textblob import TextBlob

nltk.download('punkt')
nltk.download('wordnet')

def run():
    # load files
    data_dir = "data"

    output_dir = "output"
    if not os.path.exists(output_dir): os.makedirs(output_dir)

    book_counter = 0

    all_rows = []
    books = os.listdir(data_dir)

    global_sentence_counter = 1
    for book in books:

        book_counter += 1

        # Book info from folder title
        book_meta = book.split("_")
        book_series_number = book_meta[0]
        book_author = book_meta[1].replace("-", " ").title()
        book_title = ' '.join(book_meta[5:])
        book_year = book_meta[3]

        book_dict = {
            "id": book_counter,
            "series_number": book_series_number,
            "title": book_title,
            "author": book_author,
            "year": book_year
        }

        print(book_title)

        sentence_counter = 1

        for filename in glob.glob(data_dir + '/' + book + '/*.txt'):

            current_page = filename.split("_")[-1].replace(".txt", "")

            # print("......... Page: " + str(current_page))

            with open(filename, 'r', encoding="latin-1") as file:
                file_content = file.read()
                file_content = re.sub('\s+', ' ', file_content).strip()

                file_content = file_content.replace("\t", " ").strip()

                sentences = sent_tokenize(file_content)
                total_sentences = len(sentences)

                c = 0
                for sentence in sentences:
                    words = word_tokenize(sentence)
                    has_verb = False

                    # Word count cutoff
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

                        sentence_dict = {
                            "id": global_sentence_counter,
                            "number": sentence_counter,
                            "text": sentence,
                            "sentiment_score": sentiment_score,
                            "page": current_page
                        }

                        row = {
                            "book": book_dict,
                            "sentence": sentence_dict
                        }
                        all_rows.append(row)

                        sentence_counter += 1
                        global_sentence_counter += 1

                    c +=1
            file.close()
        
        print(".... " + str(sentence_counter) + " sentences added")

    # Save output
    output_path = output_dir + "/books.csv"
    
    print("Writing output to: " + str(output_path))

    with open(output_path, 'w', newline='') as csvfile:
        columns = [
            'book_id',
            'book_series_number',
            'book_title',
            'book_author',
            'book_year',
            'sentence_page',
            'sentence_id',
            'sentence_number',
            'sentence_text',
            'sentence_sentiment_score',
            'sentence_lda'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()

        for row in all_rows:
            book = row['book']
            sentence = row['sentence']

            writer.writerow({
                'book_id' : book['id'],
                'book_series_number' : book['series_number'],
                'book_title' : book['title'],
                'book_author' : book['author'],
                'book_year' : book['year'],
                'sentence_page' : sentence['page'],
                'sentence_id' : sentence['id'],
                'sentence_number' : sentence['number'],
                'sentence_text' : sentence['text'],
                'sentence_sentiment_score' : sentence['sentiment_score'],
                'sentence_lda': ''
            })

    csvfile.close()

run()