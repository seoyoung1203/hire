CREATE EXTERNAL TABLE books
(
    ISBN STRING,
    Book_Title STRING,
    Book_Author STRING,
    Year INT,
    Publisher STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ";",
  "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION '/input/book/books';

CREATE VIEW books_view AS
SELECT
  ISBN,
  Book_Title,
  Book_Author,
  CAST(Year AS INT) AS Year,
  Publisher
FROM books;


CREATE EXTERNAL TABLE ratings
(
    User_ID INT,
	ISBN STRING,
    Book_Rating INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ";",
  "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION '/input/book/ratings';

CREATE VIEW ratings_view AS
SELECT
  CAST(User_ID AS INT) AS User_ID,
  ISBN,
  CAST(Book_Rating AS INT) AS Book_Rating
FROM ratings;


CREATE EXTERNAL TABLE users
(
   User_ID INT,
   Location STRING,
   Age INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ";",
  "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION '/input/book/users';

CREATE VIEW users_view AS
SELECT
  CAST(User_ID AS INT) AS User_ID,
  Location,
  CAST(Age AS INT) AS Age
FROM users;
                            


SELECT ISBN, COUNT(*) FROM books_view
GROUP BY ISBN
HAVING COUNT(*) >1;

SELECT COUNT(*) FROM users_view
WHERE age is null

SELECT MIN(Age) AS min_age,MAX(Age) AS max_age,AVG(Age) AS avg_age FROM users_view;

SELECT MIN(Year) AS min_year,MAX(Year) AS max_year,AVG(Year) AS avg_year FROM books_view;

SELECT Book_Rating, COUNT(*) AS rating_count FROM ratings_view
GROUP BY Book_Rating LIMIT 10;

SELECT * FROM ratings_view;

SELECT b.publisher, COUNT(*) AS book_count, AVG(r.book_rating) AS avg_rating
FROM books_view b  JOIN ratings_view r 
ON b.ISBN = r.ISBN
GROUP BY b.publisher
ORDER BY book_count desc
LIMIT 10;

SELECT b.book_title, COUNT(book_rating) AS rating_count, AVG(book_rating ) AS avg_rating
FROM books_view b JOIN ratings_view r
ON b.ISBN = r.isbn 
GROUP BY b.book_title
ORDER BY rating_count DESC 
LIMIT 10;

SELECT b.Year, AVG(book_rating) AS avg_rating
FROM books_view b JOIN ratings_view r
ON b.ISBN = r.isbn
GROUP BY Year;

SELECT 
	u.location, 
	AVG(r.book_rating) AS avg_rating, 
	COUNT(r.book_rating) AS rating_count
FROM users_view u JOIN ratings_view r
ON u.user_id = r.user_id
GROUP BY u.location
HAVING rating_count >= 10
ORDER BY avg_rating DESC;

SELECT 
	b.book_author, 
	AVG(r.book_rating) AS avg_rating, 
	COUNT(r.book_rating) AS rating_count
FROM books_view b JOIN ratings_view r
ON b.ISBN = r.isbn
GROUP BY b.book_author
HAVING rating_count > 10
ORDER BY avg_rating DESC
LIMIT 10;
















