# dolma-corpus-creation

The scripts here are intended to take the Dolma corpus, download the raw files, and create a 1-gram, two-grams, and 3-grams corpus for them.

First, follow the instructions here to download the raw files: [https://huggingface.co/datasets/allenai/dolma].

Next, modify the scripts/Dolma_corpus_creation.py file to create the appropriate n-gram corpus. Modify the `ngram` argument in the `main()` function to be either onegram, bigram, or trigram (will modify to take command line arguments in the future when I have time).

Thirdly, run check_corpus_integrity.py, once again changing the function to match whether you created a onegram, bigram, or trigram corpus. This script ensures that none of the .gz files are corrupted.

Finally, you can use the corpus_search.py script to search for whatever you want. Currently this script takes as its input a .csv file of ngrams that you would like to search and returns frequencies for them.
