import numpy as np
import pandas as pd
import gensim
import csv
import os
import pickle
from gensim.utils import simple_preprocess
from gensim.parsing.preprocessing import STOPWORDS
from gensim import corpora, models
import nltk
from nltk.stem.porter import *
from nltk.stem import WordNetLemmatizer, SnowballStemmer
from nltk.corpus import stopwords
from joblib import dump, load

import spacy

np.random.seed(2018)
nltk.download('stopwords')

# English stemmer
stemmer_en = SnowballStemmer('english')

# German stemmer
stemmer_de = SnowballStemmer("german")
german_stop_words = stopwords.words('german')
spacy_de = spacy.load('de')

global_c_en = 1
global_c_de = 1

glob_t_en = 1
glob_t_de = 1

def lemmatize_stemming_en(text):
    return stemmer_en.stem(WordNetLemmatizer().lemmatize(text, pos='n'))

def lemmatize_stemming_de(text):
    x = spacy_de(text, disable=["parser", "tagger", "ner"])
    return stemmer_de.stem(x[0].lemma_)

def preprocess_en(text):
    global global_c_en
    global glob_t_en

    print( str(global_c_en) + "/" + str(glob_t_en))

    result = []

    for token in gensim.utils.simple_preprocess(text):
        token = token.strip()
        if token not in gensim.parsing.preprocessing.STOPWORDS and len(token) > 3:
            result.append(lemmatize_stemming_en(token))
    global_c_en += 1

    return result

def preprocess_de(text):
    global global_c_de
    global glob_t_de
    global german_stop_words

    print( str(global_c_de) + "/" + str(glob_t_de))

    result = []
    for token in gensim.utils.simple_preprocess(text):
        if token not in gensim.parsing.preprocessing.STOPWORDS and token not in german_stop_words and len(token) > 3:
            result.append(lemmatize_stemming_de(token))
    global_c_de += 1

    return result

def run():

    global glob_t_de
    global glob_t_en

    input_csv_path = "output/books.csv"
    bow_corpus_path = "output/lda_bow_corpus.joblib"
    lda_model_path = "output/lda_model.joblib"

    print("loading the data ...")
    data = pd.read_csv(input_csv_path)

    data_en = data[ data["sentence_language"] == "en"]
    data_text_en = data_en[['sentence_text']].copy()
    #data_text_en['index'] = data_text_en.index
    documents_en = data_text_en
    glob_t_en = len(documents_en)

    data_de = data[ data["sentence_language"] == "de"]
    data_text_de = data_de[['sentence_text']].copy()
    #data_text_de['index'] = data_text_de.index
    documents_de = data_text_de

    glob_t_de = len(documents_de)

    if os.path.isfile(bow_corpus_path) and os.path.isfile(lda_model_path):
        print("loading lda model...")
        bow_corpus = load(bow_corpus_path) 
        lda_model = load(lda_model_path) 

    else:
        print("process the en data ...")
        processed_docs_en = documents_en["sentence_text"].map(preprocess_en)

        print("process the de data ...")
        processed_docs_de = documents_de["sentence_text"].map(preprocess_de)

        processed_docs = processed_docs_en.append(processed_docs_de)

        print( str(glob_t_en) + " en docs")
        print( str(glob_t_de) + " de docs")
        
        print("create dictionary of words ...")
        dictionary = gensim.corpora.Dictionary(processed_docs)
        dictionary.filter_extremes(no_below=15, no_above=0.5, keep_n=100000)

        print("create bow_corpus ...")
        bow_corpus = [dictionary.doc2bow(doc) for doc in processed_docs]
        dump(bow_corpus, bow_corpus_path)

        print("create lda_model ...")
        lda_model = gensim.models.LdaMulticore(bow_corpus, num_topics=150, id2word=dictionary, passes=2, workers=2)
        dump(lda_model, lda_model_path)

    # We are saving the lda data as arrays
    # the columns types must be set to object
    data["sentence_lda"] = ""
    data["sentence_lda"] = data["sentence_lda"].astype(object)
    data["sentence_lda_scores"] = ""
    data["sentence_lda_scores"] = data["sentence_lda_scores"].astype(object)

    total_data = len(data.index)
    c = 1


    # Get LDA topics for every sentence
    for row_index, row in data.iterrows():

        print( str(c) + "/" + str(total_data))
        doc_text = row["sentence_text"]

        lda_topics = []
        lda_scores = []

        # max topic words
        total_topic_words = 6

        # max words per topic group
        words_per_topic = 3

        for i, score in sorted(lda_model[bow_corpus[row_index]], key=lambda tup: -1*tup[1]):

            # don't get topics below this score
            # will just be bad matches
            if score > 0.1:

                topics = lda_model.print_topic(i, words_per_topic)
                topics = topics.split("+")

                for t in topics:
                    t_data = t.split("*")
                    t_score = t_data[0]
                    t_topic = t_data[1].replace("\"", "").strip()

                    if len(lda_topics) < total_topic_words:
                        lda_topics.append(t_topic)

                        # some kind of score
                        final_score = float(score) + float(t_score)
                        lda_scores.append( round(final_score, 3))

        # Update the new column values in the dataframe
        data.at[row_index, 'sentence_lda'] = lda_topics
        data.at[row_index, 'sentence_lda_scores'] = lda_scores

        c += 1

    # SAVE data
    print("Saving to csv")
    data.to_csv(input_csv_path)

###
run()