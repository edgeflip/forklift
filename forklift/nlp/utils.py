import nltk
import random


def tokenizer(message):
    return nltk.regexp_tokenize(message, r'(?u)\b\w\w+\b')


class ReservoirSampler(object):
    def __init__(self, sample_size, **kwargs):
        super(ReservoirSampler, self).__init__()
        self.sample_size = sample_size
        self.sample = []
        self.prev_value = ''
        self.prev_index = 0

    def sampling(self, generator):
        print "starting at", self.prev_value, self.prev_index
        index_addition = self.prev_index
        for index, value in enumerate(generator):
            if index % 1000 == 0:
                print "got to ", index
            index += index_addition
            self.prev_value = value
            self.prev_index = index
            if index < self.sample_size:
                self.sample.append(value)
            else:
                r = random.randint(0, index)
                if r < self.sample_size:
                    self.sample[r] = value
        return self.sample
