###./bin/spark-submit WILL_script.py

from __future__ import print_function
import sys 
from pyspark import SparkConf, SparkContext
import collections
import pandas as pd 
import matplotlib.pyplot as plt
conf = SparkConf().setMaster("local").setAppName("AssignmentWILLTUN")
sc = SparkContext(conf = conf)
from itertools import cycle, islice
import fnmatch
import operator
import re   # regular expressions
import numpy as np
import statsmodels.formula.api as sm
# 15, 73, 30  - 669 & 73


#import plotly.plotly as py
#from plotly.graph_objs import *

#ratings
#userId,movieId,rating,timestamp
ratings = sc.textFile("file:/Users/wt0062/Documents/spark-2.0.2-bin-hadoop2.7/WILL_ratings.csv")
#movies
#movieId,title,genres     GENRES 3rd field format: Crime|Drama|Mystery|Thriller 
movies = sc.textFile("file:/Users/wt0062/Documents/spark-2.0.2-bin-hadoop2.7/WILL_movies.csv")
# must read in movies twice for task 2
movies2 = sc.textFile("file:/Users/wt0062/Documents/spark-2.0.2-bin-hadoop2.7/WILL_movies.csv")



### Data wrangling

# STRIP OUT THE CSV headers - which contain data types different to some columns - causing problems with reduce functions down stream
header_r = ratings.first()    
ratings = ratings.filter(lambda line: line != header_r)
header_m = movies.first()    
movies = movies.filter(lambda line: line != header_m)


# Change the data encoding format 
ratings = ratings.map(lambda line: line.encode('utf-8').strip())
movies = movies.map(lambda line: line.encode('utf-8').strip())

# Split the data by commas 
ratings = ratings.map(lambda line: line.split(","))
movies = movies.map(lambda line: line.split(","))


ARGUMENT1_task = sys.argv[1]

''' Task 1 : Search User by ID - Show number of movies user has was watched  '''



if ARGUMENT1_task == "--TASK1":
	# creates (userID, 1)
	user_one = ratings.map(lambda x: (x[0],1)) 
	# reduce by key and for each reduction, perform a count using the second value in the datastructur eg. (count + 1)
	user_movieCounts = user_one.reduceByKey(lambda x, y: x + y)
	if (len(sys.argv) == 3):
		user_to_select = sys.argv[2] # DEFINE USER : eg. user 116
		if(user_to_select == "ALL"):
			user_movieCounts = user_movieCounts.sortByKey().foreach(print)
		else:
			specific_user_movieCount = user_movieCounts.filter(lambda x: x[0] == user_to_select)
			specific_user_movieCount.foreach(print)

	# This permits the client to select multiple user ids and to show the number of movies
	# that each user has watched
	if (len(sys.argv) > 3):	
		# eg. WILL_script.py --TASK1 116 650 60 581
		users_list = sys.argv[2:len(sys.argv)]
		specific_user_movieCount = user_movieCounts.filter(lambda x: x[0] in users_list)
		specific_user_movieCount.foreach(print)
		collected_user_movieCount = specific_user_movieCount.collect()
		pandas_user_movieCount = pd.DataFrame(collected_user_movieCount, columns=['user','movies_watched'])
		my_colors = list(islice(cycle(['b', 'r', 'g', 'y', 'k']), None, len(pandas_user_movieCount)))
		pandas_user_movieCount = pandas_user_movieCount.set_index('user')
		ax = pandas_user_movieCount.plot(kind='bar', title ="Number of movies watched", figsize=(8, 8), fontsize=10, color=my_colors, legend = False)
		ax.set_xlabel("userID", fontsize=10)
		ax.set_ylabel("number of movies watched", fontsize=10)
		plt.show()








''' Task 1.1 Search user ID - show number of genres user has watched. '''

if ARGUMENT1_task == "--TASK1.1":

	movieID_userID = ratings.map(lambda x: (x[1],x[0])) 
	movieID_genre = movies.map(lambda x: (x[0], x[-1])) 
	#(movies,(all the users that watched the movie,genre of movie)) 
	movieID_userID_genre = movieID_userID.leftOuterJoin(movieID_genre)
	def Mysplit(tupleIn):
		# function take in the tuple of userID, movieID and genre
		userIDi = tupleIn[0]     # userID
		movieIDi = tupleIn[1]     # movieID
		genrei = tupleIn[2]     # genre
		arrayOut = genrei.split("|")
		return(userIDi,arrayOut)
	# flip RDD so userID is key
	userID_movieID_genre = movieID_userID_genre.map(lambda xy : (xy[1][0], xy[0], xy[1][1]))
	# splits the genres by |, return (userID,(genre1,genre2,genre3), and groups RDD by user key 
	userID_genre = userID_movieID_genre.map(Mysplit).groupByKey() 
	# make each RDD a list of lists
	userID_genre = userID_genre.mapValues(list)

	userID_genre = userID_genre.mapValues(lambda l: [item for sublist in l for item in sublist]) #flattens the list of list into one list
	userID_genre_count = userID_genre.mapValues(lambda x: len(set(x))).sortByKey() #set removes duplicates in a list
	if (len(sys.argv) == 3):
		user_to_select = sys.argv[2] # DEFINE USER : eg. user 116
		if(user_to_select == "ALL"):
			result = userID_genre_count.collect()  
			for result in result:
			    print(result)
		else:
			userID_genre = userID_genre.filter(lambda x: x[0] == user_to_select)
			userID_genre_count = userID_genre_count.filter(lambda x: x[0] == user_to_select)
			userID_genre_count.foreach(print)
			userID_genre.mapValues(lambda x: set(x)).foreach(print) # REMOVES DUPLICATES FROM LIST WITH SET()
			#remove duplicates from a list

	if (len(sys.argv) > 3):	
	# eg. WILL_script.py --TASK1.5 116 650 60 581
		users_list = sys.argv[2:len(sys.argv)]
		userID_genre_count = userID_genre_count.filter(lambda x: x[0] in users_list)
		userID_genre_count.foreach(print)

		collected_user_genreCount = userID_genre_count.collect()

		pandas_user_genreCount = pd.DataFrame(collected_user_genreCount, columns=['user','genres_watched'])
		
		my_colors = list(islice(cycle(['b', 'r', 'g', 'y', 'k']), None, len(pandas_user_genreCount)))
		pandas_user_genreCount = pandas_user_genreCount.set_index('user')
		ax = pandas_user_genreCount.plot(kind='bar', title ="Number of genres watched", figsize=(8, 8), fontsize=10, color=my_colors, legend = False)
		ax.set_xlabel("userID", fontsize=10)
		ax.set_ylabel("number of GENRES watched", fontsize=10)
		plt.show()




''' TASK 2: Search movie by id/title, show the average rating'''

if ARGUMENT1_task == "--TASK2" or ARGUMENT1_task == "--TASK1.2" or ARGUMENT1_task == "--TASK5" or ARGUMENT1_task == "--TASK2.5" or ARGUMENT1_task == "--TASK4":
	movieID_userID = ratings.map(lambda x: (x[1],x[0])) 
	# create RDD of (movieID,(rating,1))
	movieID_ratings = ratings.map(lambda line: (line[1], (float(line[2]),1))) #.foreach(print)
	movieID_total_ratings_total_count = movieID_ratings.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])) # reduce by key - and sum ratings + sum index count 
	movieID_average_rating = movieID_total_ratings_total_count.mapValues(lambda x: x[0] / x[1])   # find average rating - movieID, average rating

### SEARCH movie BY TITLE - Link up movieID - average rating to movie title
#./bin/spark-submit WILL_script.py --TASK2 MovieTitle_Rating
#./bin/spark-submit WILL_script.py --TASK2 MovieID_Rating

	def returnMovieIDMovieTitle(valIn):
		valIn = valIn.encode('utf-8').strip()  ### FIXED THE UNICODE PROBLEM!!!!!!!!!!!
		comma_count = fnmatch.filter(valIn, ',') # this can count NUMBER of commas'''
		index_of_cell_containing_GENRES = len(comma_count)
		valIn = valIn.split(",")	
		GENRES = valIn[index_of_cell_containing_GENRES]
		MOVIEID = valIn[0]
		index_of_cell_containing_GENRES_minus_1 = index_of_cell_containing_GENRES - 1
		index_of_cell_containing_GENRES_plus_1 = index_of_cell_containing_GENRES + 1
		movieTitle = []
		for i in range(1,index_of_cell_containing_GENRES):
			movieTitle.append(valIn[i])
		# Extract movie title in the face of the multiple comma
		if (len(movieTitle) > 1):
			#movieTitle = movieTitle.reduce(lambda x,y: x+y,movieTitle)
			movieTitle = reduce(operator.add, movieTitle)      #convert movieTitle to single string
			movieTitle = ''.join(movieTitle)                  # join all strings together 
			movieTitle = movieTitle.strip('"')                #strip off the quotation marks
		else:
			movieTitle = ''.join(movieTitle)
	
		# Extact date of movie 
		index_of_cell_containing_date = index_of_cell_containing_GENRES - 1
 		cell_containing_date = valIn[index_of_cell_containing_date]  
 		all_numbers = re.findall('[0-9]', cell_containing_date) # returns an array of all numbers
 		number_array = np.asarray(all_numbers)
 		number_array_length = len(number_array)
 		if number_array_length == 0:                   
 			YEAR = number_array_length # return zero if movie does not have a date 
 		else:
 			first_number_year_index = number_array_length - 4 
 			year = number_array[first_number_year_index:number_array_length]   # return date
 			YEAR = ''.join(year)
		return(MOVIEID, (movieTitle, YEAR))             

	# STRIP OUT THE CSV headers - which contain data types different to some columns - causing problems with reduce functions down stream
	header_m2 = movies2.first()    
	movies2 = movies2.filter(lambda line: line != header_m2)

	#- (movie, (movie_Title, year))
	movieID_movieTitle_Year = movies2.map(returnMovieIDMovieTitle).cache()
	movieID_movieTitle = movieID_movieTitle_Year.map(lambda x: (x[0],x[1][0])).cache()
	movieID_average_rating_movieTitle = movieID_average_rating.join(movieID_movieTitle)
	MovieTitle_average_rating = movieID_average_rating_movieTitle.map(lambda x: (x[1][1],x[1][0])) #.foreach(print)


if ARGUMENT1_task == "--TASK2":
	ARGUMENT2_movieID = sys.argv[2] # ENTER MOVIE ID  
	if len(sys.argv) == 3:
		if ARGUMENT2_movieID == "ALL":
			movieID_average_rating_movieTitle.foreach(print)
		if ARGUMENT2_movieID == "MovieID_Rating":
			movieID_average_rating.foreach(print)
		if ARGUMENT2_movieID == "MovieTitle_Rating":
			MovieTitle_average_rating.foreach(print)
	if len(sys.argv) > 3:
		if ARGUMENT2_movieID == "MovieID_Rating":
			ARGUMENT3_movieID = sys.argv[3] # ENTER MOVIE ID  
			movieID_SEARCH = movieID_average_rating_movieTitle.filter(lambda line: line[0] == ARGUMENT3_movieID) #eg. 1265
			movieID_SEARCH.foreach(print)
		if ARGUMENT2_movieID == "MovieTitle_Rating":
			ARGUMENT3_movieID = sys.argv[3] # ENTER MOVIE TITLE
			movieID_SEARCH = movieID_average_rating_movieTitle.filter(lambda line: line[1][1] == ARGUMENT3_movieID) #Anne Frank Remembered (1995)
			movieID_SEARCH.foreach(print)

			#./bin/spark-submit WILL_script.py --TASK2 MovieTitle_Rating 'Groundhog Day (1993)'

''' TASK 2.5: Search the number of users that have watched the movie'''

if ARGUMENT1_task == "--TASK2.5" or ARGUMENT1_task == "--TASK6" :
	movieID_one = ratings.map(lambda x: (x[1],1)).sortByKey()
	movieID_total_count = movieID_one.reduceByKey(lambda x,y: x+y).cache() #reduce by key and add values together - movies watched by user
	
	if ARGUMENT1_task == "--TASK2.5":
		ARGUMENT2 = sys.argv[2] # ENTER MOVIE ID  
		if ARGUMENT2 == "ALL":
			movieID_total_count.foreach(print)
		if ARGUMENT2 == "movie_ID_select":
			ARGUMENT3_movieID = sys.argv[3]
			movieID_total_count = movieID_total_count.filter(lambda line: line[0] == ARGUMENT3_movieID) #eg. 5637
			movieID_total_count = movieID_total_count.leftOuterJoin(movieID_movieTitle) 
			movieID_total_count.foreach(print)
		if ARGUMENT2 == "movie_title_select":
			ARGUMENT3_movietitle = sys.argv[3]
			movieTitle_count = movieID_total_count.leftOuterJoin(movieID_movieTitle) 
			movieTitle_count = movieTitle_count.filter(lambda line: line[1][1] == ARGUMENT3_movietitle) # 'Pretty Woman (1990)'
			movieTitle_count = movieTitle_count.map(lambda x: (x[1][1],x[1][0])).foreach(print)
						

''' TASK 1.2 given a LIST of users, search all movies watched by each user''' 
### we will use the returnMovieIDMovieTitle function again to revisit the last part of TASK 1 
# eg. WILL_script.py --TASK1.2 116 650 60 581

if ARGUMENT1_task == "--TASK1.2":
	movieID_userID = ratings.map(lambda x: (x[1],x[0])) #.sortByKey() #.foreach(print) 
	movieID_movieTitle #.sortByKey().foreach(print)
	movieID_userID_movieTitle = movieID_userID.leftOuterJoin(movieID_movieTitle).sortByKey() #.foreach(print)

	users_list = sys.argv[2:len(sys.argv)]
	selected_userID_movieTitle = movieID_userID_movieTitle.filter(lambda x: x[1][0] in users_list)
	selected_userID_movieTitle = selected_userID_movieTitle.map(lambda x: (x[1][0], [x[1][1]])).sortByKey().foreach(print)
	#selected_userID_movieTitle = selected_userID_movieTitle.reduceByKey(lambda x,y: x+y).foreach(print)




''' TASK 3: Search genre, show all movies in that genre ''' 

def decompose_Genre(Input):
	MovieID = Input[0]
	GENRES = Input[-1]  
	allGenres = GENRES.split("|")
#	return(allGenres)
	singleGenre_movie = []   # intiialize empty list to become GENRE:MOVIE
	# create a list of [One Genre - movie ID]
	for i in range(0,len(allGenres)):
		singleGenre_movie.append(allGenres[i])
		singleGenre_movie.append(MovieID)
##	return (allGenres, yIn)
	#turn single_Genre_movie list into nest list of [[genre1, movieId][genre2,movieID]]
	i=0
	nested_list=[]
	while i<len(singleGenre_movie):
#  		nested_list.append(singleGenre_movie[i:i+2])  # within nested list, for each PAIR(2) of GENRE:MOVIE, append to the nest_list
  		A = singleGenre_movie[i]  #genre
  		B = singleGenre_movie[i+1] #movie ID
#  		d = {}
 # 		d[A] = B
 		C = (A,B)
  		nested_list.append(C)
  		i+=2

	return(nested_list)




if ARGUMENT1_task == "--TASK3":
	#./bin/spark-submit WILL_script.py --TASK4
	GenreMovie = movies.map(lambda x : decompose_Genre(x))#.foreach(print)   list of genres that each movie belongs to
	flat_GenreMovie = GenreMovie.flatMap(lambda x: x) #.foreach(print)   # convert list of list into single list RDD
	list_of_MOVIES_per_GENRE = flat_GenreMovie.groupByKey().mapValues(list) #.foreach(print) (genre, [all movies in genre])
	list_of_MOVIES_per_GENRE_count = list_of_MOVIES_per_GENRE.map(lambda x: (x[0], len(x[1]))).foreach(print)  # Return count of total movies in genre
	
	''' Task 3.5: Given a list of genres, search all movies belonging to each genre.''' 
	#./bin/spark-submit WILL_script.py --TASK4 'Children'
	if len(sys.argv) == 3:
		ARGUMENT2_genre_list = sys.argv[2] #eg. 'Children'
		Movies_in_selected_Genre = list_of_MOVIES_per_GENRE.filter(lambda line: line[0] == ARGUMENT2_genre_list)
		Movies_in_selected_Genre.foreach(print)	 # prints all movies in genre 

	if (len(sys.argv) > 3):	
	# eg. WILL_script.py --TASK3 'Children' 'Drama'
		genre_list = sys.argv[2:len(sys.argv)]
		Movies_in_selected_Genre = list_of_MOVIES_per_GENRE.filter(lambda line: line[0] in genre_list)
		Movies_in_selected_Genre.foreach(print)




''' TASK 4: Search movies by year '''

# movie id, movie title, year 
#movieID_movieTitle_Year.map(lambda x: (x[0],x[1][0],x[1][1]))
if ARGUMENT1_task == "--TASK4":
	if len(sys.argv) == 3:
		ARGUMENT2_year_selection = sys.argv[2] #eg. '2002'
		year_movieTitle = movieID_movieTitle_Year.map(lambda x: (x[1][1], x[1][0]))
		year_movieTitle = year_movieTitle.groupByKey().mapValues(list) #.foreach(print)
		Movies_in_selected_Year = year_movieTitle.filter(lambda line: line[0] == ARGUMENT2_year_selection).foreach(print)
	else:
		year_movieTitle = movieID_movieTitle_Year.map(lambda x: (x[1][1], x[1][0]))
		Movies_in_all_years = year_movieTitle.groupByKey().mapValues(list).foreach(print)

''' TASK 5: List the top n movies with highest rating, ordered by the rating'''

# eg.   ./bin/spark-submit WILL_script.py --TASK5 40
if ARGUMENT1_task == "--TASK5":
	number_of_movies_to_select = int(sys.argv[2])
	sorted_average_rating_MovieID = movieID_average_rating.map(lambda x: (x[1], x[0])).sortByKey(False)  # sorted - average rating sorted in DESCENDING order, movieID
	top_rated_movies = sorted_average_rating_MovieID.take(number_of_movies_to_select)  # takes top n elements of RDD and returns a LIST
	#print(top_rated_movies)
	# VISUALIZE
	sparkDF = pd.DataFrame(top_rated_movies, columns=['average rating', 'movieID'])
	sparkDF = sparkDF.set_index('movieID')
	ax = sparkDF.plot(kind='bar', title ="Movies Ranked", figsize=(8, 8), fontsize=10)
	ax.set_xlabel("movie ID", fontsize=10)
	ax.set_ylabel("average rating", fontsize=10)
	plt.show()
	print(sparkDF)


''' TASK 6: List the top n movies with the highest number of watches, ordered by the number of watches'''
#  ./bin/spark-submit WILL_script.py --TASK6 10    <- would take top 10 most watched movies



if ARGUMENT1_task == "--TASK6":
	number_of_movies_to_select = int(sys.argv[2])
	ranked_movies_by_watches = movieID_total_count.map(lambda x: (x[1],x[0])).sortByKey(False) #.foreach(print)
	top_watched_movies = ranked_movies_by_watches.take(number_of_movies_to_select)
	#print(top_watched_movies)
	# VISUALIZE
	sparkDF = pd.DataFrame(top_watched_movies, columns=['number of watches','movieID'])
	sparkDF = sparkDF.set_index('movieID')
	ax = sparkDF.plot(kind='bar', title ="Movies Ranked", figsize=(8, 8), fontsize=10)
	ax.set_xlabel("movie ID", fontsize=10)
	ax.set_ylabel("number of watches", fontsize=10)
	plt.show()
	print(sparkDF)









'''    ##### PART 2 #####    ''' 

# small sample size = positive detection is less likely 


''' Task 7: Find the favourite genre of a given user, a group of users. Define how you will define favourte '''  

### so I will divide number of movies watched in a genre by 10 [x] to normalize this variable
### and i will multiple each genres "number of movies watched" by "average rating of movie in genre"
### to obtain each users "fav genre score"
### Then I will apply a loop so that the genre with the highest "fav score" is returned for each user

# eg. ./bin/spark-submit WILL_script.py --TASK7 478 591 339

if ARGUMENT1_task == "--TASK7" or ARGUMENT1_task == "--TASK9" or ARGUMENT1_task == "--TASK10":
	movieID_user_rating = ratings.map(lambda x: (x[1],(x[0],float(x[2])))) #.foreach(print)
	movieID_title_genre = movies.map(lambda x: (x[0],(x[1],x[-1].split('|')))) #.foreach(print)
	ALL = movieID_user_rating.leftOuterJoin(movieID_title_genre) #.foreach(print) ## movieID, ((user, rating), (title, [genre]))    eg. ('3198', (('531', 2.0), ('Papillon (1973)', 'Crime|Drama')))

	
	#MovieID_user_Rating = userMovieRating.map(lambda x: (x[1],(x[0],x[2])))

	def decompose_Genre2(Input):
		movieID = Input[0]
		user = Input[1][0][0]
		rating = Input[1][0][1]
		genre = Input[1][1][1]
	
		empty_list = []                 #[User, Genre1, rating... User, GenreN, rating] 
		for i in range(0,len(genre)):
			empty_list.append(user)
			empty_list.append(genre[i])
			empty_list.append(rating)
		i=0
		nested_list=[]
		while i<len(empty_list):
#  		nested_list.append(singleGenre_movie[i:i+2])  # within nested list, for each PAIR(2) of GENRE:MOVIE, append to the nest_list
  			A = empty_list[i]  #user
  			B = empty_list[i+1] #genre n 
  			C = empty_list[i+2] # rating
  			UserID_AND_Genre = A + B
  			D = (UserID_AND_Genre, C)
  			nested_list.append(D)
  			i+=3
  		return(nested_list)

	nested_list_UserGenre_rating = ALL.map(lambda x: decompose_Genre2(x)) #.foreach(print)   # [(userID1genre1, rating1),...,(userID1genere_N, rating1)]
	flattened_UserGenre_rating= nested_list_UserGenre_rating.flatMap(lambda x: x) #.foreach(print)   # convert list of list into single list RDD
	userGenre_list_ratings= flattened_UserGenre_rating.groupByKey().mapValues(list) #.foreach(print) #(User_genre, [all ratings of that number])

	# This function finds the average rating per UserGenre-concatenated. 
	# This function also Decouples the concatenatedUserGenre into separate fields again
	import re
	def averageRating(Input):
		userGenre =Input[0]
		rating_list = Input[1]
		num_movies_watched_in_genre = len(rating_list)
		averageRating = reduce(lambda x, y: x + y, rating_list) / num_movies_watched_in_genre
		userID = int(filter(str.isdigit, userGenre))
		GenreOut = ''.join([i for i in userGenre if not i.isdigit()])
		return(userID, (GenreOut, averageRating, num_movies_watched_in_genre))

	# returns (user, (specific_genre, average rating, num_movies_watched_in_genre)  eg. (273, ('Romance', 4.318181818181818, 11))
	user_Genre_averageRatingPerGenre = userGenre_list_ratings.map(lambda x: averageRating(x)) #.foreach(print)
	# returns key: user + values: list of ('genre1', rating1, num_watchs1), ('genre2', rating2, num_watches2)
	user_Genre_averageRatingPerGenre = user_Genre_averageRatingPerGenre.groupByKey().mapValues(list) #.foreach(print)

	def rankFavGenre(Input):
		userID = Input[0] # key: user
		TheList = Input[1] #list of genre stats eg. ('genre1', rating1, num_watchs1), ('genre2', rating2, num_watches2)
	# if the list of genres is just one, simply return the userID, genre and rating
		if len(TheList) == 1:
			Genre = TheList[0][0]
			av_rating = TheList[0][1]
			num_movies_in_genre = float(TheList[0][2]) / 10.00 # TYPE INT --> float
			fav_score = av_rating * num_movies_in_genre
			return (userID,(Genre,fav_score))
		else:
			#If user has watched more than one genre
			outList = []
			for i in range(0,len(TheList)):			
				Genre = TheList[i][0]
				# Normalize average rating of each genre with number of movies in genre watched 
				av_rating = TheList[i][1] 
				num_movies_in_genre = float(TheList[i][2]) / 10.00  # TYPE INT --> float
				fav_score = av_rating * num_movies_in_genre      #FAVOURITE = NORMALIZED rating * Normalized num of movies in genre
				outList.append((Genre,fav_score))
				# ^ collect a list of genre and fav score ... eg. [(Comedy, 3.4),(Drama, 4.3)]
			initial_genre= outList[0][0]
			initial_av_rating= outList[0][1]
			for i in range(1, len(outList)): 
				genre_posterior = outList[i][0]
				rating_posterior = outList[i][1]
				# THIS CONDITION FINDS THES GENRE WITH THE HIGHEST FAV SCORE! 
				# It says, if the assessing genre has better fav score than one held by initial variable, replace variable!
				if rating_posterior > initial_av_rating:
					initial_av_rating = rating_posterior
					initial_genre = genre_posterior
			return(userID, (initial_genre,initial_av_rating))	 

	user_Genre_averageRatingPerGenre = user_Genre_averageRatingPerGenre.map(lambda x: rankFavGenre(x)) #.foreach(print)  # returns  
	# display

	if len(sys.argv) >= 3:
		users_to_select = sys.argv[2:len(sys.argv)]
		users_to_select = [ int(x) for x in users_to_select ]
		if ARGUMENT1_task != "TASK10":
			out = user_Genre_averageRatingPerGenre.filter(lambda line: line[0] in users_to_select)
			out = out.collect()
			for i in range(1,len(out)):
				print(out[i])
	else:
		user_Genre_averageRatingPerGenre.foreach(print) # This returns a user, her favourite genre --> based on average score 
		

	if ARGUMENT1_task != "--TASK10":
		if ARGUMENT1_task != "--TASK9":
			if len(sys.argv) > 2:  
				ARGUMENT2_select_user = int(sys.argv[2])
				fav_genre_of_selected_Genre = user_Genre_averageRatingPerGenre.filter(lambda line: line[0] == ARGUMENT2_select_user) #Eg. 499
				fav_genre_of_selected_Genre.foreach(print)	 # prints all movies in genre 		
			else:
				user_Genre_averageRatingPerGenre.foreach(print)

 

''' TASK 8: Compare the movie tastes of two users. Consider and justify how to compare and present data: '''
# ./bin/spark-submit WILL_script.py --TASK8 671 669

# I will calculate the COSINE Similarity between two selected users 
# So two users will be represented as two vectors
# and cosine similarity will be the innter product space + which measures the cosine of the angle between the two "vectors/users"



if ARGUMENT1_task == "--TASK8":
	#create RDD, (movie,(user, rating))
	movie_user_rating = ratings.map(lambda x: (x[1],(x[0],float(x[2])))) #.foreach(print) 	
	# intialize users to compare
	user1_to_compare = sys.argv[2] #eg. 671
	user2_to_compare = sys.argv[3] #eg. 669

	# select user data 
	user1_movie_user_rating = movie_user_rating.filter(lambda x: x[1][0] == user1_to_compare)
	user2_movie_user_rating = movie_user_rating.filter(lambda x: x[1][0] == user2_to_compare)

	# join the users data by common movie
	# output: (movie, ((user1, rating),(user2, rating)))
	joined_movie_user_rating = user1_movie_user_rating.join(user2_movie_user_rating) #.foreach(print)  # joins by common key - removes non common keys




	### We can EXTRA: VISUALIZE 
	### Scatterplot of ^
	pre_visual = joined_movie_user_rating.map(lambda x: (x[0],x[1][0][1], x[1][1][1]))
	sparkDF = pre_visual.collect()
	sparkDF = pd.DataFrame(sparkDF, columns=['movieID', 'user1_rating', 'user2_rating'])
	#print(sparkDF)
	#sparkDF.plot.scatter(x='user1_rating', y='user2_rating');

	# fit_fn is now a function which takes in x and returns an estimate for y
	plt.scatter(sparkDF['user1_rating'], sparkDF['user2_rating'], s=10)

	# put in linear regression line to show similarity between two users 
	coeffs = np.polyfit(sparkDF['user1_rating'],sparkDF['user2_rating'],1) #linear regression
	m = coeffs[0]
	b = coeffs[1]
	plt.plot(sparkDF['user1_rating'], m*sparkDF['user1_rating'] + b, '-')

	# finish plotting graph 
	plt.xlabel('user1_rating of movie')
	plt.ylabel('user2_rating of movie')
	plt.title('compare ratings of two users')
	plt.show()
	p = np.poly1d(coeffs)
	


	#### WE CAN COMPARE SIMILARITY BY FITTING AN ORDINARY LEAST SQUARES REGRESSION
	#### OF MOVIE RATINGS
	least_squares_regression = sm.ols(formula="user2_rating ~ user1_rating", data=sparkDF).fit()
	print(least_squares_regression.summary())

	### OR WE CAN COMPUTE COSINE SIMILARITY 

	# extract just rating pairs
	rating_pairs = joined_movie_user_rating.map(lambda x: (x[1][0][1], x[1][1][1])) #.foreach(print)
	#(movie1, movie2) = > (rating1, rating2)

	def computeCosineSimilarity_stage1(ratingPairs):
		for i in ratingPairs:
			vec_x = ratingPairs[0]
    		vec_y = ratingPairs[1]
    		xx = vec_x * vec_x
    		yy = vec_y * vec_y
    		distance_vecx_vecy = vec_x * vec_y 
    		return(("key", (xx, yy, distance_vecx_vecy, 1)))

	cosineStage1 = rating_pairs.map(lambda x: computeCosineSimilarity_stage1(x)) #.foreach(print)
	# result = total of :
	# "key", (xx, yy, xy, count) 

	cosineStage2 = cosineStage1.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1], x[2]+x[2], x[3]+x[3])) #.foreach(print)
	output =cosineStage2.take(1)
	#print(output[0][1])
	similarity_score = output[0][1][2]/(output[0][1][0]*output[0][1][1])  # sum(xy)/(sum(xx)+sum(yy))
	# TASK COMPLETE
	print("Cosine Similarity score: ", similarity_score)  # The LOWER the cosine similarity score, the more similar the two user tastes




'''    ##### PART 3 #####    ''' 

''' TASK 9: CLUSTER USERS BY MOVIE TASTE ''' 


if ARGUMENT1_task == "--TASK9":

	# method 1: 
	# movie taste can be defined as favourite genre 
	# i will cluster users via favourite genre 
	favGenre_user = user_Genre_averageRatingPerGenre.map(lambda x: (x[1][0], x[0])) #.foreach(print)
	clustered_users = favGenre_user.groupByKey().mapValues(list).foreach(print)



''' TASK 10: PROVIDE MOVIE RECOMMENDATIONS - user X might like movie ABC and so might like movie XYZ ''' 
# We will cluster users via their favourite genre
# We will find the specified user and link her to the person who scored the genre most similarly 
# then we will find the top rated movies in the specified genre
# find the ones that they DO NOT have in common - then recommend!



if ARGUMENT1_task == "--TASK10":
	user_to_select = int(sys.argv[2]) # DEFINE USER : eg. user 116

	# return, user, (genre, rating of SELECTED USER) 
	selected_user_and_stats = user_Genre_averageRatingPerGenre.filter(lambda x: x[0] == user_to_select) # .foreach(print) 
	# favourite genre of selected user
	selected_users_fav_genre = selected_user_and_stats.map(lambda x: x[1][0]) #.foreach(print)
	selected_users_fav_genre = selected_users_fav_genre.collect()


	selected_users_fav_score = selected_user_and_stats.map(lambda x: x[1][1]) #.foreach(print)
	selected_users_fav_score = selected_users_fav_score.collect()
	selected_users_fav_score = selected_users_fav_score[0]
#	print(selected_users_fav_score)


	user_Genre_averageRatingPerGenre = user_Genre_averageRatingPerGenre.map(lambda x: (x[0], ((x[1][0]),x[1][1]))) #.foreach(print)
	favGenre_user = user_Genre_averageRatingPerGenre.map(lambda x: (x[1][0], ((x[0]),x[1][1]))) #.foreach(print)
	#genre, (list of (users, ratings))
	clustered_users = favGenre_user.groupByKey().mapValues(list) #.foreach(print)

	# a flat list containing genre + all users in that genre,ratings
	common_users_by_genre = clustered_users.filter(lambda x: [x[0]] == selected_users_fav_genre) #.foreach(print)
	
	# This function, loops through all the users who shares the favourite genre 
	# of the selected user 
	# then it calculates a distant measure
	# to find the users who like the selected user genre as much as they do
	def findSimilarUsers(inputIn):
		genreIn = inputIn[0]
		theList = inputIn[1]
		distance_measure_of_genresFavScore = []
		# take index + distance measure of fav score
		for i in range(0,len(theList)):
			each_user = theList[i][0]
			each_fav_score = theList[i][1]
			distance_measure = abs(each_fav_score - selected_users_fav_score) 
			distance_measure_of_genresFavScore.append(distance_measure)

		sorted_distance_measures = sorted(distance_measure_of_genresFavScore)
		similar_user_distance_scores = sorted_distance_measures[1:4]  # find 3 nearest neighbours
		index_of_similar_distances_in_original_list = []
		for i in range(0, len(similar_user_distance_scores)):
			original_index = distance_measure_of_genresFavScore.index(similar_user_distance_scores[i])
			index_of_similar_distances_in_original_list.append(original_index)
		#return(distance_measure_of_genresFavScore)
		#return(index_of_similar_distances_in_original_list)

		similar_user_list = []
		for i in range(0, len(index_of_similar_distances_in_original_list)):
			data_on_similar = theList[index_of_similar_distances_in_original_list[i]][0]
			similar_user_list.append(data_on_similar)

		return(similar_user_list)


	similar_users = common_users_by_genre.map(lambda x: findSimilarUsers(x)) #.foreach(print)
	similar_users = similar_users.collect()
	similar1 = similar_users[0][0] 
	similar2 = similar_users[0][1]
	similar3 = similar_users[0][2]

	# find favourite movies of similar users
	# create a list of movies seen by selected user
	# find which movies that has not been by selected user

	# userID, movies, rating, time stamp
	similar_user_movie_rating1 = ratings.filter(lambda x: int(x[0]) == similar1)
	similar_user_movie_rating2 = ratings.filter(lambda x: int(x[0]) == similar2)
	similar_user_movie_rating3 = ratings.filter(lambda x: int(x[0]) == similar3)
	selected_user_movie_rating = ratings.filter(lambda x: int(x[0]) == int(user_to_select))
	
#	similar_user_movie_rating1.foreach(print) # prints all the movies watched by the user deemed similar
	# find highest ratest movies - above a threshold of rating 4
    # make a list of these 
	TOP_recommendations_similar_user_movie_rating1 = similar_user_movie_rating1.filter(lambda x: float(x[2]) >= float(4.0)) 
	TOP_recommendations_similar_user_movie_rating2 = similar_user_movie_rating2.filter(lambda x: float(x[2]) >= float(4.0))
	TOP_recommendations_similar_user_movie_rating3 = similar_user_movie_rating3.filter(lambda x: float(x[2]) >= float(4.0))

	#make movies the key
	TOP_recommendations_movie_rating1 = TOP_recommendations_similar_user_movie_rating1.map(lambda x: (x[1], x[2]))
	TOP_recommendations_movie_rating2 = TOP_recommendations_similar_user_movie_rating2.map(lambda x: (x[1], x[2]))
	TOP_recommendations_movie_rating3 = TOP_recommendations_similar_user_movie_rating3.map(lambda x: (x[1], x[2]))
	# the movie, rating of the selected user
	selected_user_movie_rating = selected_user_movie_rating.map(lambda x: ((x[1], x[2])))


	### SUBTRACT BY KEY - permits us to find the keys which are found in the pre-recommendation RDDs but not in the user selected RDD
	recommendation_1 = TOP_recommendations_movie_rating1.subtractByKey(selected_user_movie_rating) #.foreach(print) # found in TOP_recommendations BUT NOT selected_user
	recommendation_2 = TOP_recommendations_movie_rating2.subtractByKey(selected_user_movie_rating) #.foreach(print)
	recommendation_3 = TOP_recommendations_movie_rating3.subtractByKey(selected_user_movie_rating) #.foreach(print)

    # union permits union by common key - and collecting all the ratings for similar keys/movies
	recommendation_12 = recommendation_1.union(recommendation_2)
	recommendation_123 = recommendation_12.union(recommendation_3) 
	recommendation_123.groupByKey().mapValues(list).foreach(print)  # FINAL RECOMMENDATION ratings given by other users


