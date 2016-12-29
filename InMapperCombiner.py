from collections import defaultdict
from mrjob.job import MRJob


class InMapperCombiner(MRJob):
    def mapper_init(self):
        self.H = defaultdict(int)

    def mapper(self, _, corpus):
        for word in corpus.strip().split('\n'):
            self.H[word] += 1

    def mapper_final(self):
        for key, value in self.H.iteritems():
            yield key, value

    def reducer(self, word, count):
        yield word, sum(count)


if __name__ == '__main__':
    InMapperCombiner.run()

