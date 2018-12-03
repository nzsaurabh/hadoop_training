# python script for map reduce to find most popular movies and sort by popularity
# input: movie database containing userID, movieID, rating, timestamp
# output: Movie IDs and number of ratings (popularity) sorted by popularity
# author: Saurabh Gupta adapted from the Hadoop course by Frank Kane, Sundog Education

# import map reduce functions from mrjob library
# to install run on command line: pip install mrjob==0.5.11
from mrjob.job import MRJob
from mrjob.step import MRStep

# define class
# use two reducers one after the other
# first reducer totals the number of ratings and sorts by it
# second uses the first output and puts movie ID in the first column
class SortByPopularity(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_movieID,
                   reducer=self.reducer_count_movieID),
            MRStep(reducer=self.reducer_sorted_output)
			
        ]

    def mapper_get_movieID(self, _, line):
	    # split tab separated values
        (userID, movieID, rating, timestamp) = line.split('\t')
        # unique key is movie ID, values are a sequence of 1s each time it has been rated
        # e.g. 101:1,1,1,1
        yield movieID, 1

    # reducer sorts at this stage
    def reducer_count_movieID(self, key, values):
    # sum the number of 1s to get total number of ratings
    # keep values in first column so that reducer sorts by number of ratings
    # sorting is by character so pad the numbers with zeros to make them all 5 digits 
        yield str(sum(values)).zfill(5), key
	
# use for loop so that it doesn't club movies with same rating counts	
    def reducer_sorted_output(self, count, movies):
        for movie in movies:
            yield movie, count

# standard code to run the python code
if __name__ == '__main__':
    SortByPopularity.run()

# end of code