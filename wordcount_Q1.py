#!/bin/python

from mrjob.job import MRJob
from mrjob.step import MRStep
import re, heapq, logging

from mrjob.util import log_to_stream

WORD_RE = re.compile(r"[\w]+")

class MRWordCount(MRJob):           
    
    def steps(self):
        return [MRStep(
                mapper          = self.mapper_1,
                combiner        = self.combiner_1,
                reducer_init    = self.reducer_1_init,
                reducer         = self.reducer_1,
                reducer_final   = self.reducer_1_final),
                MRStep(
                reducer_init    = self.reducer_2_init,
                reducer         = self.reducer_2,
                reducer_final   = self.reducer_2_final)]

    def mapper_1(self, _, line):
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)
            
    def combiner_1(self, key, list_values):
        yield (key, sum(list_values))

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
        
        #while len(self.h)>0:
        #    count, word = heapq.heappop(self.h)
        #    yield (word, count)

if __name__ == '__main__':
    log_to_stream(level=logging.DEBUG)
    MRWordCount.run()
