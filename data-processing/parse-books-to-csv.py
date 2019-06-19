import glob
import os
import re
import csv
import nltk
from nltk.corpus import wordnet as wn
from nltk.tokenize import sent_tokenize 
from nltk.tokenize import word_tokenize
from textblob import TextBlob
from unidecode import unidecode
import pandas as pd

nltk.download('punkt')
nltk.download('wordnet')

def text_to_unicode(text):
    return unicode(text, encoding = "utf-8")

def run():
    output_dir = "output"
    if not os.path.exists(output_dir): os.makedirs(output_dir)

    # load files
    books_dir = "data/books"
    images_dir = "data/images"

    books = os.listdir(books_dir)

    book_counter = 0
    all_sentences = []
    all_images = []

    global_sentence_counter = 1
    global_images_counter = 1

    for book in books:

        book_counter += 1

        # Book info from folder title
        book_meta = book.split("_")
        book_series_number = book_meta[0]
        book_author = book_meta[1].replace("-", " ").title()
        book_title = ' '.join(book_meta[5:-1])
        book_language = book_meta[ len(book_meta)-1 ]
        book_year = book_meta[3]

        book_dict = {
            "id": book_counter,
            "language": book_language,
            "series_number": book_series_number,
            "title": book_title,
            "author": book_author,
            "year": book_year
        }

        print(book_title)

        sentence_counter = 1

        for filename in glob.glob(books_dir + '/' + book + '/*.txt'):

            current_page = filename.split("_")[-1].replace(".txt", "")

            # print("......... Page: " + str(current_page))

            with open(filename, 'r', encoding="utf-8") as file:
                file_content = file.read()
                file_content = file_content.replace("\t", " ")
                file_content = file_content.replace("\n", " ")
                file_content = file_content.replace("\r", " ")
                file_content = file_content.replace("\n\r", " ")
                file_content = re.sub(r'\s+', ' ', file_content).strip()

                sentences = sent_tokenize(file_content)
                # total_sentences = len(sentences)

                for sentence in sentences:
                    words = word_tokenize(sentence)
                    has_verb = False

                    # Word count cutoff
                    if len(words) > 10 and len([x for x in ["INDEX", "APPENDIX"] if x in words]) == 0:
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
                            "type": "text",
                            "number": sentence_counter,
                            "text": sentence,
                            "sentiment_score": sentiment_score,
                            "page": current_page
                        }

                        ##################################
                        # Is this sentence a question?
                        ##################################
                        if sentence.endswith("?"):
                            sentence_dict["type"] = "question"

                        # add sentence
                        row = {
                            "book": book_dict,
                            "sentence": sentence_dict
                        }
                        all_sentences.append(row)

                        sentence_counter += 1
                        global_sentence_counter += 1

            file.close()
        
        print(".... " + str(sentence_counter) + " sentences added")


        ######################
        ## Load images from the books (same name of folders)
        book_images_path = images_dir + '/' + book + '/images.csv'

        if os.path.exists(book_images_path):
            images_in_book = pd.read_csv(book_images_path)
            images_counter = 1

            for index,row in images_in_book.iterrows():

                image_caption = row['caption']
                image_path = row['image path']
                image_page = row['page number']

                words = word_tokenize(image_caption)
                if len(words) > 2:
                    # Sentiment Analysis
                    sentiment_score = round(TextBlob(image_caption).sentiment.polarity, 3)

                    image_dict = {
                        "id": global_images_counter,
                        "path": image_path,
                        "type": "text",
                        "number": images_counter,
                        "text": image_caption,
                        "sentiment_score": sentiment_score,
                        "page": image_page
                    }

                    ##################################
                    # Is this sentence a question?
                    ##################################
                    if image_caption.endswith("?"):
                        image_dict["type"] = "question"

                    # add image
                    row = {
                        "book": book_dict,
                        "image": image_dict
                    }
                    all_images.append(row)

                    images_counter += 1
                    global_images_counter += 1
            
            print(".... " + str(images_counter) + " images added")

    # Save Sentences and their books
    output_path = output_dir + "/books-sentences.csv"
    
    print("Writing output to: " + str(output_path))

    with open(output_path, 'w', newline='') as csvfile:
        columns = [
            'book_id',
            'book_language',
            'book_series_number',
            'book_title',
            'book_author',
            'book_year',
            'sentence_page',
            'sentence_id',
            'sentence_type',
            'sentence_number',
            'sentence_text',
            'sentence_sentiment_score'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()

        for row in all_sentences:
            book = row['book']
            sentence = row['sentence']

            writer.writerow({
                'book_id' : book['id'],
                'book_language' : book['language'],
                'book_series_number' : book['series_number'],
                'book_title' : book['title'],
                'book_author' : book['author'],
                'book_year' : book['year'],
                'sentence_id' : sentence['id'],
                'sentence_page' : sentence['page'],
                'sentence_type' : sentence['type'],
                'sentence_number' : sentence['number'],
                'sentence_text' : sentence['text'],
                'sentence_sentiment_score' : sentence['sentiment_score']
            })

    csvfile.close()


    # Save Images
    output_path = output_dir + "/books-images.csv"
    
    print("Writing output to: " + str(output_path))

    with open(output_path, 'w', newline='') as csvfile:
        columns = [
            'book_id',
            'image_page',
            'image_path',
            'image_id',
            'image_type',
            'image_number',
            'image_text',
            'image_sentiment_score'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()

        for row in all_images:
            book = row['book']
            image = row['image']

            writer.writerow({
                'book_id' : book['id'],
                'image_id' : image['id'],
                'image_page' : image['page'],
                'image_type' : image['type'],
                'image_path' : image['path'],
                'image_number' : image['number'],
                'image_text' : image['text'],
                'image_sentiment_score' : image['sentiment_score']
            })

    csvfile.close()

run()