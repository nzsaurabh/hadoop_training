# python script for map reduce to find most popular movies
# input: movie database containing userID, movieID, rating, timestamp
# output: Number of ratings i.e. popularity by Movie ID 

# import map reduce functions from mrjob library
from mrjob.job import MRJob
from mrjob.step import MRStep

# define class
class PopularityBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_movieID,
                   reducer=self.reducer_count_movieID)
        ]

    def mapper_get_movieID(self, _, line):
	# split tab separated values
        (userID, movieID, rating, timestamp) = line.split('\t')
		# unique key is movie ID, values are a sequence of 1s each time it has been rated
		# e.g. 101:1,1,1,1
        yield movieID, 1

    def reducer_count_movieID(self, key, values):
	# sum the number of 1s to get total number of ratings
        yield key, sum(values)

# standard code to run the python code
if __name__ == '__main__':
    PopularityBreakdown.run()