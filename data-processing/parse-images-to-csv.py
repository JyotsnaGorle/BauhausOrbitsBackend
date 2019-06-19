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
    all_images = []

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

    # Save Images
    output_path = output_dir + "/books-images.csv"
    
    print("Writing output to: " + str(output_path))

    with open(output_path, 'w', newline='') as csvfile:
        columns = [
            'book_id',
            'book_language',
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
                'book_language' : book['language'],
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