# import libraries
import subprocess
import sys

def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

install('pymongo')
install('numpy') 
install('pandas')
install('boto3')
install('spacy')
install('langdetect')
install('spacy')
install('gensim')
install('matplotlib')

import pymongo
import numpy as np
import pandas as pd
import zlib
import glob
import gzip

import json
import pickle

import pprint
import re

import os
import sys
import warnings

# Import AWS dependencies
import boto3

# Print timestamp of last runtime:
from time import time
from datetime import datetime
from pprint import pprint

start_time = time()

#extract files from folder, open each and append sample_data
sample_data = pd.DataFrame()
for fname in glob.glob('/home/maviewer/LDA/jpa_data/map2207_sample/*'):
#('/Users/lmcquillan/Desktop/JPA_NIH/map2207_sample/*'):
    with gzip.open(fname, 'rb') as f:
     sample_data=sample_data.append(pd.read_json(f))

sample_data.columns

data_en = sample_data.loc[sample_data['lang']=='en']

#start_time = time()
#main()
#print("--- %s seconds ---" % (time() - start_time))

#3 files totalling 968 KB = --- 0.0009000301361083984 seconds --- to import, append to df, and filter by lang==en

#python -m spacy download en_core_web_sm
import spacy #v 2.2.3
from spacy.lang.en.stop_words import STOP_WORDS
from spacy.lang.en import English
parser = English()

from spacy.cli.download import download
#download('en_core_web_sm')
nlp = spacy.load("en_core_web_sm")

tokens = []
lemmas = []
pos = []
tag = []
dep = []

for doc in nlp.pipe(data_en['text'].values, batch_size=100, n_threads=3):
    if doc.is_parsed:
        tokens.append([n.text for n in doc if not n.is_punct and not n.is_stop and not n.is_space and not n.like_url and not n.like_num])
        lemmas.append([n.lemma_ for n in doc if not n.is_punct and not n.is_stop and not n.is_space and not n.like_url and not n.like_num])
        pos.append([n.pos_ for n in doc if not n.is_punct and not n.is_stop and not n.is_space and not n.like_url and not n.like_num])
        tag.append([n.tag_ for n in doc if not n.is_punct and not n.is_stop and not n.is_space and not n.like_url and not n.like_num])
        dep.append([n.dep_ for n in doc if not n.is_punct and not n.is_stop and not n.is_space and not n.like_url and not n.like_num])
    else:
        tokens.append(None)
        lemmas.append(None)
        pos.append(None)
        tag.append(None)
        dep.append(None)

data_en['Tweet_tokens'] = tokens
data_en['Tweet_lemmas'] = lemmas
data_en['Tweet_pos'] = pos
data_en['Tweet_tag'] = tag
data_en['Tweet_dep'] = dep

# 0.0015609264373779297 seconds to this point incl import, transformation, basic pre-processing

import gensim
from gensim.corpora.dictionary import Dictionary
from gensim import corpora
from gensim.utils import simple_preprocess
from gensim.models import CoherenceModel

# Create the term dictionary of our corpus, where every unique term is assigned an index. 
dictionary = corpora.Dictionary(lemmas)

# Convert corpus into Document-Term Matrix using dictionary prepared above.
doc_term_matrix = [dictionary.doc2bow(doc) for doc in lemmas]

# Create the object for LDA model using gensim library
Lda = gensim.models.LdaModel

# Run and Train LDA model on the document term matrix.
ldamodel = Lda(doc_term_matrix, num_topics=4, random_state = 100, update_every=3, chunksize = 50, id2word = dictionary, passes=100, alpha='auto')

print(ldamodel.print_topics(num_topics=4, num_words=6))

# Compute Perplexity
print('\nPerplexity: ', ldamodel.log_perplexity(doc_term_matrix))  #lower is better, negative means prob is too small

# Compute Coherence Score
coherence_model_lda = CoherenceModel(model=ldamodel, texts=lemmas, dictionary=dictionary, coherence='c_v')
coherence_lda = coherence_model_lda.get_coherence()
print('\nCoherence Score: ', coherence_lda)

# find a better num_topics by comparing coherence scores across various values of num_topics
"""
Compute c_v coherence for various number of topics

Parameters:
----------
dictionary : Gensim dictionary
corpus : Gensim corpus
texts : List of input texts
limit : Max num of topics

Returns:
-------
model_list : List of LDA topic models
coherence_values : Coherence values corresponding to the LDA model with respective number of topics
"""
def get_coherence(dictionary, corpus, texts, limit, start=2, step=3):
    coherence_values = []
    model_list = []
    for num_topics in range(start, limit, step):
        model = Lda(doc_term_matrix, random_state = 100, update_every=3, chunksize = 50, id2word = dictionary, alpha='auto')
        #gensim.models.wrappers.LdaMallet(mallet_path, corpus=corpus, num_topics=num_topics, id2word=id2word)
        model_list.append(model)
        coherencemodel = CoherenceModel(model=model, texts=lemmas, dictionary=dictionary, coherence='c_v')
        coherence_values.append(coherencemodel.get_coherence())
    return model_list, coherence_values

#Take a 20 percent sample of lemmas
#import random
#twentyperc = ((len(lemmas))/5)
#lemma_samp = random.sample(lemmas, twentyperc)

# Can take a long time to run.
#model_list, coherence_values = get_coherence(dictionary=dictionary, corpus=doc_term_matrix, texts=lemmas, start=8, limit=40, step=4)
#print(model_list, coherence_values)
# Show graph
#import matplotlib.pyplot as plt
#limit=40; start=8; step=4;
#x = range(start, limit, step)
#plt.plot(x, coherence_values)
#plt.xlabel("Num Topics")
#plt.ylabel("Coherence score")
#plt.legend(("coherence_values"), loc='best')
#plt.show()

#--------------------
# try another way to optimize
'''
def compute_coherence_values(corpus, dictionary, k, a, b):
    lda_model = gensim.models.LdaMulticore(corpus=corpus,
                                           num_topics=10, 
                                           random_state=100,
                                           chunksize=100,
                                           passes=10,
                                           alpha=a,
                                           eta=b,
                                           per_word_topics=True)
    coherence_model_lda = CoherenceModel(model=lda_model, texts=lemmas, corpus=doc_term_matrix, dictionary=Dictionary, coherence='c_v')
    return coherence_model_lda.get_coherence()


import tqdm
grid = {}
grid['Validation_Set'] = {}
# Topics range
min_topics = 8
max_topics = 40
step_size = 4
topics_range = range(min_topics, max_topics, step_size)
# Alpha parameter
alpha = list(np.arange(0.01, 1, 0.3))
alpha.append('symmetric')
alpha.append('asymmetric')
# Beta parameter
beta = list(np.arange(0.01, 1, 0.3))
beta.append('symmetric')
# Validation sets
num_of_docs = len(doc_term_matrix)
corpus_sets = [# gensim.utils.ClippedCorpus(corpus, num_of_docs*0.25), 
               # gensim.utils.ClippedCorpus(corpus, num_of_docs*0.5), 
               gensim.utils.ClippedCorpus(doc_term_matrix, num_of_docs*0.75), 
               doc_term_matrix]
corpus_title = ['75% Corpus', '100% Corpus']
model_results = {'Validation_Set': [],
                 'Topics': [],
                 'Alpha': [],
                 'Beta': [],
                 'Coherence': []
                }
# Can take a long time to run
if 1 == 1:
    pbar = tqdm.tqdm(total=540)
    
    # iterate through validation corpuses
    for i in range(len(corpus_sets)):
        # iterate through number of topics
        for k in topics_range:
            # iterate through alpha values
            for a in alpha:
                # iterare through beta values
                for b in beta:
                    # get the coherence score for the given parameters
                    cv = compute_coherence_values(corpus=corpus_sets[i], dictionary=dictionary, k=k, a=a, b=b)
                    # Save the model results
                    model_results['Validation_Set'].append(corpus_title[i])
                    model_results['Topics'].append(k)
                    model_results['Alpha'].append(a)
                    model_results['Beta'].append(b)
                    model_results['Coherence'].append(cv)
                    
                    pbar.update(1)
    pd.DataFrame(model_results).to_csv('/home/maviewer/LDA/lda_tuning_results.csv', index=False)
    pbar.close()

'''

"""
Coherence measures the relative distance between words within a topic. 
There are two major types C_V typically 0 < x < 1 and uMass -14 < x < 14. 
It's rare to see a coherence of 1 or +.9 unless the words being measured are either identical words or bigrams. 
Like United and States would likely return a coherence score of ~.94 or hero and hero would return a coherence of 1. 
The overall coherence score of a topic is the average of the distances between words. I try and attain a .7 in my 
LDAs if I'm using c_v I think that is a strong topic correlation. I would say:

.3 is bad

.4 is low

.55 is okay

.65 might be as good as it is going to get

.7 is nice

.8 is unlikely and

.9 is probably wrong

Low coherence fixes:

adjust your parameters alpha = .1, beta = .01 or .001, seed = 123, ect
get better data
at .4 you probably have the wrong number of topics 
check out https://datascienceplus.com/evaluation-of-topic-modeling-topic-coherence/ 
for what is known as the elbow method - it gives you a graph of the optimal number of topics for 
greatest coherence in your data set. I'm using mallet which has pretty good coherance here is code to check 
coherence for different numbers of topics:
"""


#----------- aggregate by cluster and group
## assign docs to topics, get topic perc, etc

def format_topics_sentences(ldamodel=ldamodel, corpus=lemmas, texts=dictionary):
    sent_topics_df = pd.DataFrame()

    # Get dominant topic in each document
    for i, row in enumerate(ldamodel[corpus]):
        row = sorted(row, key=lambda x: (x[1]), reverse=True)
        # Get the dominant topic, percent Contribution and keywords for each doc
        for j, (topic_num, prop_topic) in enumerate(row):
            if j == 0:  # => dominant topic
                wp = ldamodel.show_topic(topic_num)
                topic_keywords = ", ".join([word for word, prop in wp])
                sent_topics_df = sent_topics_df.append(pd.Series([int(topic_num), round(prop_topic,4), topic_keywords]), ignore_index=True)
            else:
                break
    sent_topics_df.columns = ['Dominant_Topic', 'Perc_Contribution', 'Topic_Keywords']

    # Add original text to the end of the output
    contents = pd.Series(lemmas)
    sent_topics_df = pd.concat([sent_topics_df, contents], axis=1)
    return(sent_topics_df)


df_topic_sents_keywords = format_topics_sentences(ldamodel=ldamodel, corpus=doc_term_matrix, texts=dictionary)

print(df_topic_sents_keywords.head())

# Format
df_dominant_topic = df_topic_sents_keywords.reset_index()
df_dominant_topic.columns = ['Document_No', 'Dominant_Topic', 'Topic_Perc_Contribution', 'Keywords', 'Text']

# Print the first 5 rows
print(df_dominant_topic.head())

print(data_en.head())

#data_en['text'] = data_en['text'].astype(int)
df_dominant_topic['Document_No'] = df_dominant_topic['Document_No'].astype(int)
#results = pd.concat([[data_en, df_dominant_topic], join="inner")
results = pd.concat([data_en, df_dominant_topic], ignore_index=False)
results.to_csv('/home/maviewer/LDA/results.csv')
print(results.head(10))
print(results.columns)

'''
# Group top 5 sentences under each topic
sent_topics_sorteddf_mallet = pd.DataFrame()

sent_topics_outdf_grpd = df_topic_sents_keywords.groupby('Dominant_Topic')

for i, grp in sent_topics_outdf_grpd:
    sent_topics_sorteddf_mallet = pd.concat([sent_topics_sorteddf_mallet, 
                                             grp.sort_values(['Perc_Contribution'], ascending=[0]).head(1)], 
                                            axis=0)

# Reset Index    
sent_topics_sorteddf_mallet.reset_index(drop=True, inplace=True)

# Format
sent_topics_sorteddf_mallet.columns = ['Topic_Num', "Topic_Perc_Contribution", "Keywords", "Text"]

# Print top 5 rows
sent_topics_sorteddf_mallet.head()


# Number of Documents for Each Topic
topic_counts = df_topic_sents_keywords['Dominant_Topic'].value_counts()

# Percentage of Documents for Each Topic
topic_contribution = round(topic_counts/topic_counts.sum(), 4)

# Topic Number and Keywords
topic_num_keywords = df_topic_sents_keywords[['Dominant_Topic', 'Topic_Keywords']]

# Concatenate Column wise
df_dominant_topics = pd.concat([topic_num_keywords, topic_counts, topic_contribution], axis=1)

# Format
df_dominant_topics.columns = ['Dominant_Topic', 'Topic_Keywords', 'Num_Documents', 'Perc_Documents']

# Print top 5 rows
print(df_dominant_topics.head())

## analyze new text
tokens = word_tokenize(document)
topics = lda_model.show_topics(formatted=True, num_topics=num_topics, num_words=20)
pd.DataFrame([(el[0], round(el[1],2), topics[el[0]][1]) for el in lda_model[dictionary_LDA.doc2bow(tokens)]], columns=['topic #', 'weight', 'words in topic'])

###------------------------------------- Multicore for faster processing
#lda_mc = gensim.models.LdaMulticore(lemmas, num_topics=3, random_state = 100, chunksize = 50, id2word = dictionary, passes=5, workers=4, iterations = 5);
#lda_mc.print_topics(num_topics=3, num_words=6)

#get program run time
print("--- %s seconds ---" % (time() - start_time))
#--- 366.5928018093109 seconds for 3 topics---
# --- 435.21546936035156 seconds for 10 topics ---
# --- 649.3230454921722 seconds for 20 topics ---
# --- 1132.47567152977 seconds for 40 topics---
'''