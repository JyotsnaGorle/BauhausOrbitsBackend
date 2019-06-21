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

glob_language_counter = {}

global_current_language = "en"

def lemmatize_stemming_en(text):
    return stemmer_en.stem(WordNetLemmatizer().lemmatize(text, pos='n'))

def lemmatize_stemming_de(text):
    x = spacy_de(text, disable=["parser", "tagger", "ner"])
    return stemmer_de.stem(x[0].lemma_)


def preprocess(text):

    global global_current_language
    global glob_language_counter

    result = []

    if global_current_language == "en":
        global global_c_en
        global glob_t_en

        print( str(global_c_en) + "/" + str(glob_language_counter[global_current_language]) ) 

        if isinstance(text, str) and text:
            for token in gensim.utils.simple_preprocess(text):
                token = token.strip()
                if token not in gensim.parsing.preprocessing.STOPWORDS and len(token) > 3:
                    result.append(lemmatize_stemming_en(token))
        global_c_en += 1

    elif global_current_language == "de":
        global global_c_de
        global german_stop_words

        print( str(global_c_de) + "/" + str(glob_language_counter[global_current_language]) ) 

        if isinstance(text, str) and text:
            for token in gensim.utils.simple_preprocess(text):
                if token not in gensim.parsing.preprocessing.STOPWORDS and token not in german_stop_words and len(token) > 3:
                    result.append(lemmatize_stemming_de(token))
        global_c_de += 1

    return result

def run():

    global glob_language_counter

    global global_current_language

    datasets = ["books-images"]
    output_dir = "output"

    for dataset_name in datasets:

        dataset_key = "sentence" if "sentences" in dataset_name else "image"
        column_containing_text = str(dataset_key) + "_text"

        input_csv_path = str(output_dir) + "/" + str(dataset_name) + ".csv"

        print("loading the data ...")
        data = pd.read_csv(input_csv_path)

        data[str(dataset_key) + "_lda"] = ""
        data[str(dataset_key) + "_lda"] = data[str(dataset_key) + "_lda"].astype(object)
        data[str(dataset_key) + "_lda_scores"] = ""
        data[str(dataset_key) + "_lda_scores"] = data[str(dataset_key) + "_lda_scores"].astype(object)

        languages = ["en", "de"]
        for language in languages:

            global_current_language = language
            ############### English
            print(">> Language: " + str(language) )

            data_temp = data.copy()
            data_temp = data_temp[ data_temp["book_language"] == language]
            data_temp["index"] = data_temp.index
            data_text = data_temp[[column_containing_text]].copy()

            documents = data_text
            glob_t = len(documents)
            print( str(glob_t) + " docs")

            glob_language_counter[language] = str(glob_t)

            bow_corpus_path = str(output_dir) + "/" + str(dataset_name) + "_" + str(language) + "_lda_bow_corpus.joblib"
            lda_model_path = str(output_dir) + "/" + str(dataset_name) + "_" + str(language) + "_lda_model.joblib"

            if os.path.isfile(bow_corpus_path) and os.path.isfile(lda_model_path):
                print("loading lda model...")
                bow_corpus = load(bow_corpus_path) 
                lda_model = load(lda_model_path) 

            else:
                print("process data...")

                processed_docs = documents[column_containing_text].map(preprocess)

                print(".... create dictionary of words ...")
                dictionary = gensim.corpora.Dictionary(processed_docs)
                dictionary.filter_extremes(no_below=15, no_above=0.5, keep_n=100000)

                print(".... create bow_corpus")
                bow_corpus = [dictionary.doc2bow(doc) for doc in processed_docs]
                dump(bow_corpus, bow_corpus_path)

                print(".... create lda_model")
                lda_model = gensim.models.LdaMulticore(bow_corpus, num_topics=150, id2word=dictionary, passes=2, workers=2)
                dump(lda_model, lda_model_path)

            # We are saving the lda data as arrays
            # the columns types must be set to object

            c = 1

            # Get LDA topics for every sentence
            for row_index, row in data_temp.iterrows():

                real_row_index = row["index"]

                print( str(c) + "/" + str(glob_t))
                doc_text = row[column_containing_text]

                lda_topics = []
                lda_scores = []

                # max topic words
                total_topic_words = 6

                # max words per topic group
                words_per_topic = 3

                if real_row_index < len(bow_corpus):

                    x = lda_model[bow_corpus[real_row_index]]
                    if len(x) > 0:

                        for i, score in sorted(x, key=lambda tup: -1*tup[1]):

                            # don't get topics below this score
                            # will just be bad matches
                            if score > 0.3:

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
                data.at[real_row_index, str(dataset_key) + "_lda"] = lda_topics
                data.at[real_row_index, str(dataset_key) + "_lda_scores"] = lda_scores

                c += 1

    # SAVE data
    print("Saving to csv")
    data.to_csv(input_csv_path)

###
run()