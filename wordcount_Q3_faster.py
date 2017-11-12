#!/bin/python

#import itertools # new
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import heapq

from mrjob.util import log_to_stream #????
import logging  #???

from lxml import etree
from lxml.etree import XMLParser
import mwparserfromhell

from io import StringIO
from datetime import datetime


WORD_RE = re.compile(r"[\w]+")

class Top100WordsXML(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper_init  =self.mapper_0_init,               
                mapper       =self.mapper_0,                    
                reducer      =self.reducer_0                    
            ),
            MRStep(
                mapper       =self.mapper_1,                    
                combiner     =self.combiner_1,                  
                reducer_init =self.reducer_1_init,              
                reducer      =self.reducer_1,                   
                reducer_final=self.reducer_1_final              
            ), 
            MRStep(
                reducer_init =self.reducer_2_init,              
                reducer      =self.reducer_2,                   
                reducer_final=self.reducer_2_final              
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
                        text_element = mwparserfromhell.parse(text_element.text.encode('ascii', 'ignore')).strip_code()
                        yield (t, (timestamp, text_element))
                
                self.buf.truncate(0)
                self.buf.seek(0)
            except:
                self.buf.truncate(0)
                self.buf.seek(0)

    def reducer_0(self, title, values):
        time = None
        val = None
        for tup in values:
            if time is None or time < tup[0]:
                time = tup[0]
                val = tup[1]
        if val is not None:
            for x in val.splitlines():
                if x != "":
                    yield(title, x)
    
    def mapper_1(self, _, line):
        for word in WORD_RE.findall(line):
            yield (word.lower().strip(), 1)

    def combiner_1(self, word, counts):
        yield (word, sum(counts))

    def reducer_1_init(self):
        self.length = 0
        self.h = []

    def reducer_1(self, key, list_values):
        if self.length <100:
            heapq.heappush(self.h, ( int(sum(list_values)), key ) )
            self.length += 1             
        else:
            heapq.heappushpop(self.h, ( int(sum(list_values)), key ) )

    def reducer_1_final(self):
        while len(self.h)>0:
            count, word = heapq.heappop(self.h)
            yield (None, (word, count) )




    def reducer_2_init(self):
        self.length = 0
        self.h = []

    def reducer_2(self, key, list_values):
        for word, count in list_values:
            if self.length <100:
                heapq.heappush(self.h, (int(count), word) )
                self.length += 1             
            else:
                heapq.heappushpop(self.h, (int(count), word) )

    def reducer_2_final(self):
        temp_arr = heapq.nlargest(100, self.h)
        for count, word in temp_arr:
            yield word, count


    
    
if __name__ == '__main__':
    log_to_stream(level=logging.DEBUG)
    Top100WordsXML.run()