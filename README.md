A Spark MapReduce program to:-

Search for mentions of industry words in the blog authorship corpus. 
The goal here is to first find all of the possible industries in which bloggers were classified. 
Then, to search each blogger’s posts for mentions of those industries and, counting the mentions by month and year.

Download the corpus here: http://u.cs.biu.ac.il/~koppel/BlogCorpus.htm (Note: the site rate-limits the speed of the download. It will take several minutes.)
Unzip the corpus file and look at the contents of a few files before reading on. Each file in the corpus is named according to information about the blogger: user_id.gender.age.industry.star_sign.xml

Within each xml file, there is a “<date>” tag which indicates the date of a proceeding “<post>”, which contains the text of an individual blog post.

The program does the following steps:

I. Get all possible industry names:
-Creates an rdd of all the filenames.
-Uses transformation until left with only a set of possible industries
-Uses an action to export the rdd to a set and makes this a spark broadcast variable

II. Search for industry names in posts, recording by year-month: 

Creates an rdd for the contents of all files [i.e. sc.wholeTextFiles(file1,file2,...) ]
-Uses transformations to search all posts across all blogs for mentions of industries, and record the frequency each industry was mentioned by month and year. The industry names should only be matched, case insensitive, if they are next to a word boundary -- space or punctuation (e.g. “marketing” would match “I am in marketing sales” and “Marketing.” but not “I like supermarketing.” or “This is marketing5 now.”).
-Uses an action to print the recorded frequencies in this format:
[(industry1, ((year-month1, count), (year-month2, count), …),
(industry2, ((year-month1, count), (year-month2, count), …), …]