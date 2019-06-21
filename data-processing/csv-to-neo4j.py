from neo4j.v1 import GraphDatabase
import os
import pandas as pd

def add_image(tx, row):
    tx.run(
        'MATCH (b:Book {book_id: $b_id}) ' +
        'MERGE (i:Image {image_id: $i_id}) ' + 
        'SET i.path = $i_path, i.text = $i_text, i.page = $i_page, i.path = $i_path, i.type = $i_type, i.sentiment_score = $i_sentiment ' +
        'MERGE (i)-[:IS_IN]->(b)',
        b_id = int(float(row['book_id'])),
        i_id = int(float(row['image_id'])),
        i_path = row['image_path'],
        i_sentiment = row['image_sentiment_score'],
        i_type = row['image_type'],
        i_text = row['image_text'],
        i_page = row['image_page']
    )

    # LDA
    if str(row["image_lda"]) != "nan":
        lda_topics = eval(row["image_lda"])
        lda_topic_scores = eval(row["image_lda_scores"])

        if len(lda_topics) > 0 and len(lda_topics) == len(lda_topic_scores):
            c = 0
            for topic_word in lda_topics:

                # score
                topic_score = lda_topic_scores[c]

                tx.run(
                    'MERGE (t:Topic {word: $word}) ' +
                    'SET t.score = $score ' +
                    'MERGE (s:Image {image_id: $s_id}) ' + 
                    'MERGE (t)-[:IS_IN]->(s)',
                    word = topic_word,
                    score = topic_score,
                    s_id = int(float(row['image_id']))
                )

                c += 1


def add_book_and_sentence(tx, row):
    # print(row)
    tx.run(
        'MERGE (b:Book {book_id: $b_id}) ' +
        'SET b.series_number = $b_series_number, b.color = $b_color, b.author = $b_author, b.title = $b_title, b.year = $b_year ' + 
        'MERGE (s:Sentence {sentence_id: $s_id}) ' + 
        'SET s.type = $s_type, s.sentence_number = $s_number, s.color = $s_color, s.text = $s_text, s.page = $s_page, s.sentiment_score = $s_sentiment_score ' +
        'MERGE (s)-[:IS_IN]->(b)',
        b_color = '#ff0000',
        b_series_number = int(float(row['book_series_number'])),
        b_id = int(float(row['book_id'])),
        b_author = row['book_author'],
        b_title = row['book_title'],
        b_year = row['book_year'],
        b_language = row["book_language"],
        s_color = '#fbfbfb',
        s_id = int(float(row['sentence_id'])),
        s_type = row['sentence_type'],
        s_page = row['sentence_page'],
        s_number = int(float(row['sentence_number'])),
        s_text = row['sentence_text'],
        s_sentiment_score = row['sentence_sentiment_score']
    )

    # LDA
    if str(row["sentence_lda"]) != "nan":
        lda_topics = eval(row["sentence_lda"])
        lda_topic_scores = eval(row["sentence_lda_scores"])

        if len(lda_topics) > 0 and len(lda_topics) == len(lda_topic_scores):
            c = 0
            for topic_word in lda_topics:

                # score
                topic_score = lda_topic_scores[c]

                tx.run(
                    'MERGE (t:Topic {word: $word}) ' +
                    'SET t.score = $score ' +
                    'MERGE (s:Sentence {sentence_id: $s_id}) ' + 
                    'MERGE (t)-[:IS_IN]->(s)',
                    word = topic_word,
                    score = topic_score,
                    s_id = int(float(row['sentence_id']))
                )

                c += 1

def run():
    # Setup neo4j driver once
    neo4j_args = {
        'uri' : 'bolt://localhost:7688',
        'user' : 'neo4j',
        'password' : 'bauhaus'
    }
    neo4j_driver = GraphDatabase.driver(
        neo4j_args['uri'], auth = (neo4j_args['user'],neo4j_args['password'])
    )

    with neo4j_driver.session() as session:
        session.run('MATCH (n) DETACH DELETE n')
        session.run('MATCH (n:Image) DETACH DELETE n')

        # Book 'id' field is a unique key
        c_i = session.run('CREATE CONSTRAINT ON (b:Book) ASSERT b.book_id IS UNIQUE')

        # Image 'id' field is a unique key
        c_p = session.run('CREATE CONSTRAINT ON (i:Image) ASSERT i.image_id IS UNIQUE')
        
        # Sentence 'ids' field is a unique key
        c_a = session.run('CREATE CONSTRAINT ON (s:Sentence) ASSERT s.sentence_id IS UNIQUE')

        # Topic's word is a unique field
        c_a = session.run('CREATE CONSTRAINT ON (t:Topic) ASSERT t.word IS UNIQUE')


    books_path = 'output/clean-books-sentences.csv'
    print("Saving books and sentences from " + str(books_path))
    save_books_and_sentences(neo4j_driver, books_path)

    with neo4j_driver.session() as session:
        session.run('MATCH (s:Book) SET s.book_id = toInteger(s.book_id)')
        session.run('MATCH (s:Sentence) SET s.sentence_id = toInteger(s.sentence_id)')
        session.run('MATCH (s:Sentence) SET s.page = toInteger(s.page)')
        session.run('MATCH (s:Sentence) SET s.sentiment_score = toFloat(s.sentiment_score)')

    images_path = 'output/books-images.csv'
    print("Saving images from " + str(images_path))
    save_images(neo4j_driver, images_path)
    with neo4j_driver.session() as session:
        session.run('MATCH (s:Image) SET s.image_id = toInteger(s.image_id)')

    
    # Close neo4j driver
    neo4j_driver.close()

def save_images(neo4j_driver, filepath):
    with neo4j_driver.session() as session:
        df = pd.read_csv(filepath)
        total = len(df.index)
        c = 1

        for index,row in df.iterrows():
            print( str(c) + '/' + str(total))

            if str(row["image_id"]) != "nan":

                # Save row
                session.write_transaction(add_image, row)

                c += 1

def save_books_and_sentences(neo4j_driver, filepath):

    with neo4j_driver.session() as session:
        df = pd.read_csv(filepath)

        total = len(df.index)
        c = 1
        for index,row in df.iterrows():
            print( str(c) + '/' + str(total))

            # Save row
            session.write_transaction(add_book_and_sentence, row)

            c += 1
run()