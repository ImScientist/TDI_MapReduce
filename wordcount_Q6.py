from mrjob.job import MRJob
from mrjob.step import MRStep
import re


from mrjob.util import log_to_stream #????
import logging  #???

from lxml import etree
from lxml.etree import XMLParser
import mwparserfromhell

from io import StringIO
from datetime import datetime


parselink = re.compile(r'\[\[(.*?)(\]\]|\|)')

class LinksStats(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper_init  =self.mapper_0_init,
                mapper       =self.mapper_0,
                mapper_final =self.mapper_0_final,
                reducer      =self.reducer_0
            ),
            MRStep(
                reducer      =self.reducer_1
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
                        
                        timestamp  = datetime.strptime(time_element.text, "%Y-%m-%dT%H:%M:%SZ")
                        links_list = mwparserfromhell.parse(text_element.text.encode('ascii', 'ignore')).filter_wikilinks()
                        links_set  = set()
                        
                        for link in links_list:
                            link_stripped = parselink.search(unicode(link)).groups()[0]
                            if link_stripped is not None:
                                links_set.add(link_stripped)
                        
                        yield (t, (timestamp, len(links_set)))
                
                self.buf.close()
                self.buf = StringIO()
            except:
                #self.buf.truncate(0)
                #self.buf.seek(0)
                self.buf.close()
                self.buf = StringIO()

    # final eaddiiton
    def mapper_0_final(self):
        self.buf.close()

    def reducer_0(self, title, tuples):
        time = None
        n_links = None
        for tup in tuples:
            if time is None or time < tup[0]:
                time = tup[0]
                n_links = tup[1]
        if n_links is not None:
            yield n_links, 1



    def reducer_1(self, n_links, list_of_ones):
        yield None, (n_links, sum(list_of_ones))


    
    def reducer_2_init(self):
        self.N_links_tot=0
        self.N_pages=0
        self.links_counts_arr=[]
        self.p=[]
        self.v=[]

    def reducer_2(self, _, tuples):
        for n_links, count in tuples:
            self.N_links_tot += n_links*count
            self.N_pages += count
            self.links_counts_arr.append( (n_links, count) )
            
    def reducer_2_final(self):
        self.links_counts_arr.sort()
        for links, count in self.links_counts_arr:
            self.p.append( float(count)/self.N_pages )
            self.v.append( links )
        
        
        for p_i,v_i in zip(self.p,self.v):
            yield p_i,v_i


if __name__ == '__main__':
    log_to_stream(level=logging.DEBUG)
    LinksStats.run()