
from __future__ import print_function
from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

"""
The following example is of collaborative filtering using ALS algorithm 
to build the recommendation model and evaluate it on training data.
"""


if __name__ == '__main__':
    sc = SparkContext(appName="PySpark MLlib Example")
    data = sc.textFile("data/test.data")
    ratings = data.map(lambda l: l.split(",")).\
        map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))

    print("Ratings -> {}".format(ratings))

    # Build the recommendation model using Alternating Least Squares
    rank = 10
    numIterations = 10
    model = ALS.train(ratings, rank, numIterations)

    # Evaluate the model on training data
    testdata = ratings.map(lambda p: (p[0], p[1]))
    predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
    rates_and_preds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
    print("Rates and predictions -> {}".format(rates_and_preds))

    MSE = rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
    print("Mean Squared Error = {}".format(MSE))

    # Save and load model
    model.save(sc, "target/tmp/myCollaborativeFilter")
    sameModel = MatrixFactorizationModel.load(sc, "target/tmp/myCollaborativeFilter")
