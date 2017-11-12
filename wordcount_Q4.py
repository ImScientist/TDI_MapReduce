#!/bin/python

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

from lxml import etree
from lxml.etree import XMLParser
import mwparserfromhell

from io import StringIO
from datetime import datetime

import numpy as np


WORD_RE = re.compile(r"[\w]+")

class NGramEntropy(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper_init  =self.mapper_0_init,
                mapper       =self.mapper_0,
                reducer_init =self.reducer_0_init,
                reducer      =self.reducer_0
            ),
            MRStep(
                combiner     =self.combiner_1,
                reducer      =self.reducer_1
            ),
            MRStep(
                combiner     =self.combiner_2,
                reducer      =self.reducer_2
            ),
            MRStep(
                mapper       =self.mapper_3,
                reducer      =self.reducer_3
            )
        ]

    def mapper_0_init(self):
        self.buf = StringIO()
    
    def mapper_0(self, _, line):
        line = line.decode("utf-8")    # convert byte string to an unicode string;   unicode(line, encoding) does the same
        self.buf.write(line)
        if re.search(r'</page>', line):
            try:
                magical_parser = XMLParser(encoding='utf-8', recover=True)
                root = etree.parse(StringIO(self.buf.getvalue()), magical_parser).getroot()
                t = root.find("title").text
                for rev in root.findall("revision"):
                    
                    time_element = rev.find("timestamp")
                    text_element = rev.find("text")
                    if time_element is not None and text_element is not None:
                        
                        timestamp    = datetime.strptime(time_element.text, "%Y-%m-%dT%H:%M:%SZ")
                        text_element = mwparserfromhell.parse(text_element.text.encode('ascii', 'ignore')).filter_text()#.strip_code()
                        text_element = " ".join(" ".join(fragment.value.split()) for fragment in text_element)
                        
                        yield (t, (timestamp, text_element))
                
                self.buf.truncate(0)
                self.buf.seek(0)
            except:
                self.buf.truncate(0)
                self.buf.seek(0)

    def reducer_0_init(self):
        self.NGRAM_SIZE = 1

    def reducer_0(self, title, values):
        time = None
        val = None
        for tup in values:
            if time is None or time < tup[0]:
                time = tup[0]
                val = tup[1]
        if val is not None:
            for i in range(len(val) - (self.NGRAM_SIZE - 1)):
                yield val[i:i+self.NGRAM_SIZE], 1
            
            
    def combiner_1(self, ngram, counts):
        yield ngram, sum(counts)

    def reducer_1(self, ngram, counts):
        yield sum(counts), 1




    def combiner_2(self, count, list_ones): # list_ones = every element represents a word that appears the same amount of times
        yield count, sum(list_ones)

    def reducer_2(self, count, list_ones):
        yield count, sum(list_ones)



        
    def mapper_3(self, count, n_words_with_this_count):
        yield None, (count, n_words_with_this_count)

    def reducer_3(self, _, pairs):
        N = 0
        S = 0
        for (count, n_words_with_this_count) in pairs:
            N += n_words_with_this_count * count
            S += n_words_with_this_count * (count * np.log2(count))
        yield 'entropy', (np.log2(N) -  (S/N)) / np.log2(2)




if __name__ == '__main__':
    NGramEntropy.run()