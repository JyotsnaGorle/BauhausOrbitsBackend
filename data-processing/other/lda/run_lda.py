from neo4j.v1 import GraphDatabase
from neo4j.v1 import CypherError
import pandas as pd
import gensim
from gensim.utils import simple_preprocess
from gensim import corpora, models
from gensim.parsing.preprocessing import STOPWORDS
from nltk.stem import WordNetLemmatizer, SnowballStemmer
from nltk.stem.porter import *
import numpy as np
import nltk
import os

nltk.download('wordnet')
stemmer = SnowballStemmer("english")

# Connect to neo4j driver
driver_user = "neo4j"
driver_pass = "neo4jps"
driver = GraphDatabase.driver("bolt://localhost:7687", auth=(driver_user, driver_pass), encrypted=False)

output_path = "papers.csv"
df = ""

# Save papers
if not os.path.exists(output_path):
    with driver.session() as session:

        results = list(session.run(
            "MATCH (p:Paper) " +
            "WHERE p.paperAbstract IS NOT NULL " +
            "RETURN p.paperAbstract AS paperAbstract, p.paperId AS paperId LIMIT 10" 
        ))

        i = 0
        final_dict = {
            "paperId": [],
            "paperAbstract": []
        }
        df = ""
        for record in results:
            abstract = record["paperAbstract"].replace("\n"," ")
            abstract = abstract.replace("\r"," ")

            final_dict["paperId"].append(record["paperId"])
            final_dict["paperAbstract"].append(abstract)

        df = pd.DataFrame.from_dict(final_dict)
        df = df.dropna()

        df.to_csv(output_path)
else:
    # Read from cached papers
    df = pd.read_csv(output_path)

def lemmatize_stemming(text):
    return stemmer.stem(WordNetLemmatizer().lemmatize(text, pos='v'))

def preprocess(text):
    result = []
    for token in gensim.utils.simple_preprocess(text):
        if token not in gensim.parsing.preprocessing.STOPWORDS and len(token) > 3:
            result.append(lemmatize_stemming(token))
    return result

col_to_process = 'paperAbstract'

df_temp = df.copy()
data_text = df_temp[col_to_process]

processed_docs = data_text.map(preprocess)
dictionary = gensim.corpora.Dictionary(processed_docs)

bow_corpus = [dictionary.doc2bow(doc) for doc in processed_docs]

# bow_doc_test = bow_corpus[0]
# for i in range(len(bow_doc_test)):
#     print("Word {} (\"{}\") appears {} time.".format(bow_doc_test[i][0], dictionary[bow_doc_test[i][0]], bow_doc_test[i][1]))


# tfidf = models.TfidfModel(bow_corpus)
# corpus_tfidf = tfidf[bow_corpus]

# lda_model = gensim.models.LdaMulticore(bow_corpus, num_topics=10, id2word=dictionary, passes=2, workers=2)
# lda_model_tfidf = gensim.models.LdaMulticore(corpus_tfidf, num_topics=10, id2word=dictionary, passes=2, workers=4)

# unseen_document = 'this is a test'
# bow_vector = dictionary.doc2bow(preprocess(unseen_document))
# for index, score in sorted(lda_model_tfidf[bow_vector], key=lambda tup: -1*tup[1]):
#     print("Score: {}\t Topic: {}".format(score, lda_model_tfidf.print_topic(index, 5)))

#     # gensim
#     print(text_data)
#     dictionary = corpora.Dictionary(text_data)
#     dictionary.save('data/dictionary.gensim')
#     corpus = [dictionary.doc2bow(text) for text in text_data]
#     pickle.dump(corpus, open('data/corpus.pkl', 'wb'))

#     NUM_TOPICS = 5
#     ldamodel = gensim.models.ldamodel.LdaModel(corpus, num_topics = NUM_TOPICS, id2word=dictionary, passes=15)
#     ldamodel.save('data/model5.gensim')

#     topics = ldamodel.print_topics(num_words=4)
#     for topic in topics:
#         print(topic)

#     new_doc = 'Practical Bayesian Optimization of Caregiver Machine Learning Algorithms'
#     new_doc = prepare_text_for_lda(new_doc)
    
#     new_doc_bow = dictionary.doc2bow(new_doc)
#     print(ldamodel.get_document_topics(new_doc_bow))


# driver.close()