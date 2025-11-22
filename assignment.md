# Assignment

## Brief

Write the Python codes for the following questions.

## Instructions

Paste the answer as Python in the answer code section below each question.

### Question 1

Question: From the `movies` collection, return the documents with the `plot` that starts with `"war"` in acending order of released date, print only title, plot and released fields. Limit the result to 5.

Answer:

```python
stage_match_start_with = {
        "$match": {
            # Find documents where the 'plot' field starts with "war" (case-insensitive)
            "plot": {"$regex": "^war", "$options": "i"}
        }
    }
stage_sort = {
    
        "$sort": {
            # Sort by 'released' date in ascending order (1)
            "released": 1
        }
    }

stage_project = {
        "$project": {
            # Include these fields
            "title": 1,
            "plot": 1,
            "released": 1,
            # Exclude the default _id field
            "_id": 0
        } 
    }
stage_limit_5 = {
        "$limit": 5
    }   

pipeline = [
        stage_match_start_with,
        stage_sort,
        stage_project,
        stage_limit_5
    ]

results = movies.aggregate(pipeline)

for doc in results:
        # The 'released' field is typically a datetime object in BSON, 
        # so we convert it to a string for clean printing.
        released_date = doc.get('released').strftime("%Y-%m-%d") if doc.get('released') else "N/A"
        
        print("-" * 50)
        print(f"  Title:    {doc.get('title', 'N/A')}")
        print(f"  Released: {released_date}")
        print(f"  Plot:     {doc.get('plot', 'N/A')[:100]}...") # Truncate plot for display
#!/usr/bin/env python3

```

### Question 2

Question: Group by `rated` and count the number of movies in each.

Answer:

```python
stage_group_rated = {
    "$group": {
        # Group by the 'rated' field. '$rated' references the field value.
        "_id": "$rated",
        # Count the number of documents in each group and store it in 'count'
        "count": {"$sum": 1} 
    }
}

stage_sort_count_desc = {
    "$sort": {
        # Sort the results by the count in descending order (highest count first)
        "count": -1
    }
}

pipeline = [
    stage_group_rated,
    stage_sort_count_desc
]

results = movies.aggregate(pipeline)
if results:
    print("\n Movie Counts by MPAA Rating:")
    print("-" * 35)
    
    # Print header for table-like output
    print(f"{'Rating'} | {'Movie Count'}")
    print("-" * 35)
    
    # Iterate and print the grouped results
    for doc in results:
        # Use a sensible default if _id (the rating) is None/null
        rating = doc.get('_id', 'Unrated or Missing')
        count = doc.get('count', 0)
        
        print(f"{rating:} | {count:}")
    print("-" * 35)
    
else:
    print("\n No rating data found.")
```

### Question 3

Question: Count the number of movies with 3 comments or more.

Answer:

```python
stage_group_id = {
    "$group": {
        "_id": "$movie_id",  # Group by the ID of the movie
        "comment_count": {"$sum": 1} # Count the total comments for this movie
    }
} 

stage_add_comment_count = {
    "$match": {
        "comment_count": {"$gte": 3}
    }
}

stage_count_movies = {
    "$count": "movies_with_3_or_more_comments"
}
pipeline = [
    stage_group_id,
    stage_add_comment_count,
    stage_count_movies
]

results = list(movies.aggregate(pipeline))

if results:
    count = results[0]['movies_with_3_or_more_comments']
    print(f"\nTotal number of movies with 3 or more comments: {count}")
else:
    print("\nNo movies found with 3 or more comments.")
```

## Submission

- Submit the URL of the GitHub Repository that contains your work to NTU black board.
- Should you reference the work of your classmate(s) or online resources, give them credit by adding either the name of your classmate or URL.
