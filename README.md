# netflix-ratings
imdb ratings, rotten tomatoes audience score and critics score for netflix titles

Every time I am looking for what to watch next on netflix, I manually check the rotten tomatoes score of the netflix show, as well as its imdb score.
The aim of this project is to combine the imdb score, the rotten tomatoes score, as well as metadata related to a netflix show in a clean way.
The result can potentially be used by an app to display all the info related to a netflix show!

### Data
We use three main sources of data:
- netflix: https://www.kaggle.com/shivamb/netflix-shows
- imdb: https://datasets.imdbws.com/
- rotten tomatoes: https://www.kaggle.com/stefanoleone992/rotten-tomatoes-movies-and-critic-reviews-dataset
Data exploration and dictionary can be found in the notebook `final_project.ipynb`

### Data Model
Start schema with a bridge. 
The dimension table is the titles, containing all the scores information.
The reason for the bridge is that a person can have multiple roles in the show, this will allow us to normalize the table a bit further.
![Alt text](img/netflix-ratings.png?raw=true "data model")

### Pipeline, Data cleaning and noteworthy steps
We follow an ETL pipeline:
- Extract all the data and loading into staging tables
- Transform the tables 
- Load into final tables

As for the data cleaning and transformation steps:
- Rows with missing values for writers/directors in the imdb set are dropped
- Only kept title types `('tvSpecial', 'tvSeries', 'tvShort', 'movie', 'tvMovie', 'short', 'tvMiniSeries')` from the imdb dataset given the nature of the netflix shows
- Split genres on ',' and fill up separate table with the values
- The role table should only contain `('director', 'writer', 'actor', 'actress')`
- The title table is the main table. We Join imdb, netflix and rt tables whenever :
    - We find a lowercase matching title (somewhat a fuzzy match) 
    - And at least either a director or an actor match
    - And the year of release should match
    - Netflix left join everything, in case we don't find a title/score match, we'd still want to keep the netflix info

### Addressing Scenarios:
*If the data was increased by 100x*:
- Definitely not use my local machine. I could alternatively
- Use s3 storage due to its scalability and accessibility by other AWS services as well as redshift with a big cluster with multiple nodes
- Or given that Apache Spark is linearly scalable, We may simply add the number of clusters to increase the performance. 
- With AWS EMR we can adjust the size and number of clusters as we see fit.

*If the pipelines were run on a daily basis by 7am.*:
- Not really applicable in my case, but assuming we had the individual scores instead of the average, we can use a scheduler such as Airflow scheduler
to monitors all tasks and all DAGs, and triggers the task instances to run every day with `schedule_interval='@daily'`, and we'd only drop the dimension tables and append to the fact table.

*The database needed to be accessed by 100+ people*:
-  In order to scale horizontally, we need to add redundancy and make sure the data is available in multiple availability zones (to increase application availability).
We can do that by using the Multi-AZ feature of `RDS`. RDS will create a standby database instance in a different AZ and replicate data to it synchronously.
RDS will take the heavy lifting, we would only need to include retry logic into the database connection. 
Additionally, to scale the database tier, we can use `Amazon RDS Read Replicas`.
Horizontal scaling with RDS Multi-AZ, and RDS Read Replicas will allow the system to scale pretty far.
Additional information can be found [here](https://aws.amazon.com/blogs/startups/scaling-on-aws-part-2-10k-users/)
- Furthermore, we would need to manage the user access to the database and the resources. To do so we can use `AWS Single Sign-On` to create users, organize them in groups, and set permissions across those groups.
More information on AWS Single Sign-On can be found [here](https://aws.amazon.com/blogs/security/how-to-create-and-manage-users-within-aws-sso/)

### Example queries
Is there a correlation between:
- imdb and critics score?
- critics score and audience score?
![Alt text](img/correlation.png?raw=true "Correlation")

Recommend horror movies released after 2016 with rotten tomatoes critics score higher than 70, ordered by imdb score
![Alt text](img/recommendation.png?raw=true "Correlation")

Please refer to the notebook `final_project.ipynb`

### Choice of technology
- Wanted to practice using postgresql.
- The data model is well defined, so I thought a data warehouse would be more appropriate
- Started building airflow dags but ran out of time. I'll finish that after the course's deadline
![Alt text](img/airflow_graph.png?raw=true "Airflow DAG")


### Future steps
Due to time limits, and being a full timer, I won't have time to complete the airflow pipelines. 
I'll finish them after the submission of the project. What's left:
    - Move data to s3
    - Use redshift as a data warehouse
    - Run pipelines on schedule using airflow
    
### Steps to run the project
- Install the requirements: `pip install -r requirements.txt`
- Start airflow: `sh airflow/start.sh`
