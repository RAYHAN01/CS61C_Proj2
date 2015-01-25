from pyspark import SparkContext
import Sliding, argparse

def bfs_map(value):
    """ YOUR CODE HERE """
    # This function is the argument of flatMap method, and it should return a list.
    # format of value: (pos, level) pair
    rtn = [value] 
    # Should not change the pos in previous level, the rtn list contains value itself
    if (level <= value[1]): 
    # Check if the value pair exceeds the level. If it is larger than level, we need
    # append rtn with all of it's children.
    	child = Sliding.children(WIDTH, HEIGHT, value[0])
    	for _ in child:
    		rtn.append((_, value[1]+1))
    return rtn

def bfs_reduce(value1, value2):
    """ YOUR CODE HERE """
    return min(value1, value2)
    # This function is the argument of reduceByKey method.
    # bfs_reduce: exclude the identical pos in rdd by only leaving one with smaller level.

def exchange(value):
	return (value[1], value[0])
    # This function is the argument of map method.
    # After we find out all the solution, we need switch back from (pos, level) to (level, pos).


def solve_sliding_puzzle(master, output, height, width):
    """
    Solves a sliding puzzle of the provided height and width.
     master: specifies master url for the spark context
     output: function that accepts string to write to the output file
     height: height of puzzle
     width: width of puzzle
    """
    # Set up the spark context. Use this to create your RDD
    sc = SparkContext(master, "python")

    # Global constants that will be shared across all map and reduce instances.
    # You can also reference these in any helper functions you write.
    global HEIGHT, WIDTH, level

    # Initialize global constants
    HEIGHT = height
    WIDTH = width
    level = 0 # this "constant" will change, but it remains constant for every MapReduce job

    # The solution configuration for this sliding puzzle. You will begin exploring the tree from this node
    sol = Sliding.solution(WIDTH, HEIGHT)

    """ YOUR MAP REDUCE PROCESSING CODE HERE """

    previous = 0 # num of element in previous rdd
    curr = 1 # num of element in rdd
    # The increment of curr from previous is the condition of while loop.
    # If it is larger than 0, it means we find new (pos, level), and we keep looping.
    # Otherwise, the while loop ends since there is no new (pos, level) pair.
    pos_to_level = [(sol, 0)]
    rdd = sc.parallelize(pos_to_level).partitionBy(16)
    # intitialization of rdd and partition by 16, a number we find can boost up our speed.
    while curr - previous > 0:
    	rdd = rdd.flatMap(bfs_map, True).\
    	      flatMap(bfs_map, True).\
    	      flatMap(bfs_map, True).\
    	      flatMap(bfs_map, True).reduceByKey(bfs_reduce, 16)
              # We do 4 flatMap same time to avoid using count(), a costly method, every time.
    	previous = curr
    	curr = rdd.count()
    	level += 4
        # Since we do 4 flatMap, level increases by 4 every time.
    	
    level_to_pos = rdd.map(exchange, True).sortByKey(True).collect()
    # Exchange rdd(pos_to_level) to a new rdd contains level_to_pos.
    # Then the new one is SortByKey, so that (level, pos) pair is in order as level 
    # increasing from the first pos,(0, sol), to final pos.
    # Finally, we collect() to make a list and assign it to level_to_pos
    
    """ YOUR OUTPUT CODE HERE """

    # We use a while loop to do the output job.
    i = 0
    while i < len(level_to_pos):
        output(str(level_to_pos[i][0]) + " " + str(level_to_pos[i][1]))
        i += 1

    sc.stop()

""" DO NOT EDIT PAST THIS LINE

You are welcome to read through the following code, but you
do not need to worry about understanding it.
"""

def main():
    """
    Parses command line arguments and runs the solver appropriately.
    If nothing is passed in, the default values are used.
    """
    parser = argparse.ArgumentParser(
            description="Returns back the entire solution graph.")
    parser.add_argument("-M", "--master", type=str, default="local[8]",
            help="url of the master for this job")
    parser.add_argument("-O", "--output", type=str, default="solution-out",
            help="name of the output file")
    parser.add_argument("-H", "--height", type=int, default=2,
            help="height of the puzzle")
    parser.add_argument("-W", "--width", type=int, default=2,
            help="width of the puzzle")
    args = parser.parse_args()


    # open file for writing and create a writer function
    output_file = open(args.output, "w")
    writer = lambda line: output_file.write(line + "\n")

    # call the puzzle solver
    solve_sliding_puzzle(args.master, writer, args.height, args.width)

    # close the output file
    output_file.close()

# begin execution if we are running this file directly
if __name__ == "__main__":
    main()
