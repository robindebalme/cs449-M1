# Dependencies

````
    sbt >= 1.4.7
````

Should be available by default on the IC Cluster. Otherwise, refer to each project installation instructions.

# Usage

## Compute predictions

````
    sbt 'runMain predict.Predictor'
````

## Compute recommendations
````
    sbt 'runMain recommend.Recommender'
````

## Package for submission

Update the ````name````, ````maintainer```` fields of ````build.sbt````, with the correct Milestone number, your ID, and your email.

Package your application:
````
    sbt 'show dist'
````

You should should see an output like:
````
[info] Your package is ready in [...]/target/universal/m1_your_id-1.0.zip
````

Combine this package, along side your report and any other files mentioned in the Milestone description (see Section ````Deliverables````). Submit to the TA for grading.

# References

Essential sbt: https://www.scalawilliam.com/essential-sbt/

