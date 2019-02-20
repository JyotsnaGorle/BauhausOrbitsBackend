from neo4j.v1 import GraphDatabase
import os
import csv

def add_row(tx, row):
    tx.run(
        'MERGE (b:Book {book_id: $b_id}) ' +
        'SET b.author = $b_author, b.title = $b_title, b.year = $b_year ' + 
        'MERGE (s:Sentence {sentence_id: $s_id}) ' + 
        'SET s.text = $s_text, s.page = $s_page, s.sentiment_score = $s_sentiment_score ' +
        'MERGE (s)-[:IS_IN]->(b)',
        b_id = row['book_id'],
        b_author = row['book_author'],
        b_title = row['book_title'],
        b_year = row['book_year'],
        s_page = row['sentence_page'],
        s_id = row['sentence_id'],
        s_text = row['sentence_text'],
        s_sentiment_score = row['sentence_sentiment_score']
    )

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

    input_file = 'output/books.csv'

    with neo4j_driver.session() as session:
        # Book 'id' field is a unique key
        c_p = session.run('CREATE CONSTRAINT ON (b:Book) ASSERT b.book_id IS UNIQUE')
        
        # Sentence 'ids' field is a unique key
        c_a = session.run('CREATE CONSTRAINT ON (s:Sentence) ASSERT s.sentence_id IS UNIQUE')

        with open(input_file, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)

            c = 1
            total = sum(1 for row in reader)
            csvfile.seek(0)
            reader = csv.DictReader(csvfile)
            
            for row in reader:
                print( str(c) + '/' + str(total))

                # Save row
                session.write_transaction(add_row, row)
                c += 1

        csvfile.close()

    # Close neo4j driver
    neo4j_driver.close()

run()