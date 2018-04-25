from mrjob.job import MRJob
from mrjob.step import MRStep
import os
import time

t00=time.time()

keyColumn = 1

class MatrixMultiplication(MRJob):

    f = open('OutputMrJob.txt', 'w')

    def mapper(self, _, line):
        # This function automatically reads in lines of code
        value = line
        line = line.split("|")
        # line = list(map(int, line))
        key = line[keyColumn]

        filename = os.environ['mapreduce_map_input_file']

        value = line[0:keyColumn]+line[keyColumn+1:]
        value = "|".join(value)
        print("Key is:" + key)
        print("Value is" + value)
        if '1' in filename:
            yield key, (0, value)
        elif '2' in filename:
            yield key, (1, value)

    def reducer(self, keys, values):
        matrix0 = []
        matrix1 = []
        for value in values:
            if value[0] == 0:
                matrix0.append(value)
            elif value[0] == 1:
                matrix1.append(value) 
        for mat0, val0 in matrix0:
            for mat1, val1 in matrix1:
                joinedValue = val1  + val0
                yield(keys, joinedValue)
                self.f.write(str(keys) + "|" + str(joinedValue) + "\n")

    def steps(self): return [
        MRStep(mapper=self.mapper,
                reducer=self.reducer), 
    ]

if __name__ == '__main__':
    t0 = time.time()
    MatrixMultiplication.run()
    t1 = time.time()

    totalWithoutWrite = t1 - t0
    print ("Total time for algorithmA: " + str(totalWithoutWrite))
    #print ("Total time for algorithmA with writing to file: " + str(totalWithWrite))