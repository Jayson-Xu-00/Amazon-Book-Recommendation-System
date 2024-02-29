CREATE SCHEMA books;

USE books;

CREATE TABLE `books`.`Books_rating` (
	`Id` text, 
    `rTitle` text, 
    `User_id` text, 
    `profileName` text,
    `Score` double,
    `Time` int,
    `review_Title` text, 
    `review_Text` text, 
    `n_helpful_review` int, 
    `n_views` int
);

CREATE TABLE `books`.`Books_data` (
	`dTitle` text,
    `description` text,
    `authors` text,
    `image` text,
    `previewLink` text,
	`publisher` text,
    `publishedDate` text,
	`infoLink` text,
	`categories` text,
	`ratingsCount` int
);

SHOW GLOBAL VARIABLES LIKE 'local_infile';
SET GLOBAL local_infile = TRUE;

LOAD DATA LOCAL INFILE 'C:/Users/jayso/Desktop/Emory/Fall 2023/Managing Big Data/Final Project/Amazon Book Reviews/cleaned_Books_rating.csv' 
INTO TABLE `books`.`Books_rating`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE 'C:/Users/jayso/Desktop/Emory/Fall 2023/Managing Big Data/Final Project/Amazon Book Reviews/cleaned_Books_data.csv' 
INTO TABLE `books`.`Books_data`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

##########################################################################################################################################################
# ER Model & Tables Creation

-- Creating the Users table, import from rating
CREATE TABLE `Users` (
    `UserID` VARCHAR(255) PRIMARY KEY,
    `ProfileName` text
);

-- Creating the Reviews and Ratings table, import from rating
CREATE TABLE `Reviews_and_Ratings` (
    `ReviewID` int AUTO_INCREMENT PRIMARY KEY,
    `BookID` int,
    `UserID` VARCHAR(255),
    `Score` double,
    `Time` int,
    `ReviewTitle` text,
    `ReviewText` text,
    `NHelpfulReview` int,
    `NViews` int,
    FOREIGN KEY (`BookID`) REFERENCES `Books`(`BookID`),
    FOREIGN KEY (`UserID`) REFERENCES `Users`(`UserID`)
);

-- Creating the Authors table, import from data
CREATE TABLE `Authors` (
    `AuthorID` int AUTO_INCREMENT PRIMARY KEY,
    `authors` text
);

-- Creating the Publishers table, import from data
CREATE TABLE `Publishers` (
    `PublisherID` int AUTO_INCREMENT PRIMARY KEY,
    `publisher` text
);

-- Creating the Categories table, import from data
CREATE TABLE `Categories` (
    `CategoriesID` int AUTO_INCREMENT PRIMARY KEY,
    `Categories` text
);

-- Creating the Books table, import from data
CREATE TABLE `Books` (
    `BookID` int PRIMARY KEY,
    `Title` text,
    `Description` text,
    `Image` text,
    `PreviewLink` text,
    `PublishedDate` text,
    `InfoLink` text,
    `RatingsCount` int,
    `AuthorID` int,
    `PublisherID` int,
	`CategoriesID` int,
    FOREIGN KEY (`BookID`) REFERENCES `BookID`(`Reviews_and_Ratings`),
    FOREIGN KEY (`AuthorID`) REFERENCES `Authors`(`AuthorID`),
    FOREIGN KEY (`PublisherID`) REFERENCES `Publishers`(`PublisherID`),
    FOREIGN KEY (`CategoriesID`) REFERENCES `Categories`(`CategoriesID`)
);

##########################################################################################################################################################

#s3://big-data-jayson/final_project/amzbkrv/Books_rating.csv

# Create a new HDFS directory
hadoop fs -mkdir /user/hadoop/books

# Load data from S3 into HDFS
s3-dist-cp --src s3://big-data-jayson/final_project/amzbkrv/Books_rating.csv --dest /user/hadoop/books/
s3-dist-cp --src s3://big-data-jayson/final_project/amzbkrv/books_data.json --dest /user/hadoop/books/

s3-dist-cp --src s3://big-data-jayson/final_project/books_data_raw_test.json --dest /user/hadoop/books/
#hadoop fs -cp s3://big-data-jayson/final_project/amzbkrv/Books_rating.csv /user/hadoop/books

# PySpark Code:
df = spark.read.csv("s3://big-data-jayson/final_project/books_data_raw_test.json", header=True, inferSchema=True)
df.createOrReplaceTempView("books_data")

query = """

"""

result_df = spark.sql(query)
result_df.show()

