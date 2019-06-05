# CMPE255_Event_Rec
CMPE 255 Project, Event Reccomendation for Kaggle competition

We tried 2 approaches; a database approach and one in RAM. The Database approach was the iteration that we decided to go with 
due to the fact that construcing a mini database in ram is very time consuming during the debugging process.

The steps to run are as follows:

MongoDB Version:
1. download kaggle data (https://www.kaggle.com/c/event-recommendation-engine-challenge/data), extract to directory with py files
2. open event_popularity_benchmark.csv and run a find and replace for "L" with "" (This is to format the file correctly for the DB)
3. setup MongoDB
4. load data into DB via the "loaddata.py" file
5. Run Python3 "Event Reccomendation.py"
6. Events reccomended are written to output.csv

RAM Verson:
(Warning: This iteration is experimental and uses LOTS of RAM, and may cause your PC to freeze. I was able to run most of it with 
16GB but your result may not be the same. If memory is an issue, I suggest the MongoDB iteration)

1. download kaggle data (https://www.kaggle.com/c/event-recommendation-engine-challenge/data)
2. put all CSV files into the same directory as "Event Reccomendation RAM.py"
3. Run "Event Reccomendation RAM.py"

It should be noted that we've reduced the training set to a small percentage of the original set in the MongoDB iteration 
for the sake of runnability, but building the database will still take some time.

Please contact us if you have any questions, and we appreaciate your review of this project.
