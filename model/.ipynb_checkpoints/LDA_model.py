import numpy as np
import gensim
from gensim.utils import simple_preprocess
from gensim.parsing.preprocessing import STOPWORDS
from nltk.stem import WordNetLemmatizer, SnowballStemmer
from nltk.stem.porter import *
import numpy as np
import csv
from gensim import corpora, models
import pandas as pd


np.random.seed(2018)

# with open("/home/saeed/Job/Hiwi/BauhausOrbitsBackend/data-processing/data/wassily-kandinsky_from-point-line-to-plane.txt", "r") as input_file:
#     input_text = input_file.read()
stemmer = SnowballStemmer('english')

def lemmatize_stemming(text):
    return stemmer.stem(WordNetLemmatizer().lemmatize(text, pos='v'))

def preprocess(text):
    result = []
    for token in gensim.utils.simple_preprocess(text):
        if token not in gensim.parsing.preprocessing.STOPWORDS and len(token) > 3:
            result.append(lemmatize_stemming(token))
    return result

with open("/home/saeed/Job/Hiwi/BauhausOrbitsBackend/model/books.csv") as csvfile:
    reader = csv.DictReader(csvfile)
#     data = pd.read_csv('/model/books.csv', error_bad_lines=False)

    # data = pd.read_csv('/model/books.csv', error_bad_lines=False)

    for raw in reader:
    #     # raw_text = raw['sentence_text']
        print(type(raw))
    #     data_text = raw['sentence_text']
    #     data_text['index'] = data_text.index
    #     documents = data_text
    #     processed_data = documents['sentence_text'].map(preprocess)

    # print("Hi, check it now")
    # data_text = data['sentence_text']
    # data_text['index'] = data_text.index
    # documents = data_text
    # processed_docs = documents['sentence_text'].map(preprocess)
    #
    # dictionary = gensim.corpora.Dictionary(processed_data)
    #
    # dictionary.filter_extremes(no_below=15, no_above=0.5, keep_n=100000)
    #
    # bow_corpus = [dictionary.doc2bow(doc) for doc in processed_docs]

    # tfidf = models.TfidfModel(bow_corpus)
    #
    # corpus_tfidf = tfidf[bow_corpus]

    # lda_model = gensim.models.LdaMulticore(bow_corpus, num_topics=10, id2word=dictionary, passes=2, workers=2)

    # print("check")
    # for idx, topic in lda_model.print_topics(-1):
    #     print('Topic: {} \nWords: {}'.format(idx, topic))

