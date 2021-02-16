# Dependencies

````
    sbt >= 1.4.7
````

Should be available by default on the IC Cluster. Otherwise, refer to each project installation instructions.

# Dataset

Download the ````ml-100k.zip```` dataset in the ````data/```` folder:
````
> mkdir data
> cd data
> wget http://files.grouplens.org/datasets/movielens/ml-100k.zip   
````

Check the integrity of the file with (it should give the same number as below):
````
> md5 -q ml-100k.zip
0e33842e24a9c977be4e0107933c0723 
````

Unzip:
````
> unzip ml-100k.zip
````

# Personal Ratings

Add your ratings in the 'data/personal.csv' file, by providing a numerical rating between [1,5] for at least 20 movies. For example, to rate the 'Toy Story' movie with '5', modify this line:

````
1,Toy Story (1995),
````

to this:
````
1,Toy Story (1995),5
````

Do include your own ratings in your final submission so we can check your answers against those provided in your report.

# Usage

## Compute predictions

````
> sbt "runMain predict.Predictor --train data/ml-100k/u1.base --test data/ml-100k/u1.test --json answers.json"
````

## Compute recommendations
````
> sbt 'runMain recommend.Recommender'
````

## Package for submission

Update the ````name````, ````maintainer```` fields of ````build.sbt````, with the correct Milestone number, your ID, and your email.

Package your application:
````
> sbt 'show dist'
````

You should should see an output like:
````
[info] Your package is ready in [...]/target/universal/m1_your_id-1.0.zip
````

Combine this package, alongside your report and any other files mentioned in the Milestone description (see Section ````Deliverables````). Submit to the TA for grading.

# References

Essential sbt: https://www.scalawilliam.com/essential-sbt/

