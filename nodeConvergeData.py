# -*- coding: utf-8 -*-
"""
Created on Wed Mar 29 17:50:32 2017

@author: R00156026
"""
import sys

from sklearn import cross_validation
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import ExtraTreesClassifier
from sklearn import preprocessing
import pandas as pd
from sklearn import decomposition
import numpy as np
import matplotlib.pyplot as plt
# from sklearn.neighbors import KNeighborsClassifier
# from sklearn.manifold import TSNE
# from sklearn.metrics import classification_report, confusion_matrix
# from pandas.tools.plotting import table
# import seaborn

pd.set_option('display.max_columns', 100)
plt.style.use("ggplot")


def clusterConverge(df):
    cols = df.columns

    dok32 = sorted([i for i in cols if "dok32" in i])
    dok33 = sorted([i for i in cols if "dok33" in i])
    dok34 = sorted([i for i in cols if "dok34" in i])

    pairs = zip(dok32, dok33, dok34)

    for p in list(pairs)[:34]:
        p = list(p)
        a = df[p]
        name = ('max ' + p[0][5:])
        name1 = ('min ' + p[0][5:])
        name2 = ('std ' + p[0][5:])
        df[name] = a.max(1)[:].astype(float)
        df[name1] = a.min(1)[:].astype(float)
        df[name2] = a.std(1)[:].astype(float)

    df1 = df

    for col in dok32:
        df1 = df1.drop(col, axis=1)

    for col2 in dok33:
        df1 = df1.drop(col2, axis=1)

    for col3 in dok34:
        df1 = df1.drop(col3, axis=1)

    return df1


def standardizeData(data):

    scalingObj = preprocessing.StandardScaler()
    data2 = scalingObj.fit_transform(data)

    return data2


def extraTrees(data, target):

    model = ExtraTreesClassifier(random_state=0, n_estimators=100)
    results = model.fit(data, target)

    featureImportance = (results.feature_importances_)

    return(featureImportance)


def RandomForest(data, target):
    accScores = []

    for n in range(50, 500, 25):

        rf = RandomForestClassifier(n_estimators=n, criterion='gini',
                                    max_depth=None, oob_score=False, class_weight="balanced")
        scores = cross_validation.cross_val_score(rf, data, target, cv=10)
        preds = cross_validation.cross_val_predict(rf, data, target, cv=10)
        # print preds
        accuracy = scores.mean()
        accScores.append(accuracy)

    indexes = n

    plt.plot(indexes, accScores)
    plt.xticks(indexes)
    plt.show()

    return preds


def RFfeatureRed(data, target, df):

    dataStand = standardizeData(data)
    features = extraTrees(dataStand, target)
    sFeat = sorted(features)
    # print(sFeat)
    sortedFeatures = np.argsort(features)
    # print(sortedFeatures)

    # scores = []
    # NoFeatures = []

    x = 100
    newFeatures = sortedFeatures[0:x]
    keptFeatures = sortedFeatures[x:]
    newData = np.delete(dataStand, newFeatures, axis=1)
    featureScore = sFeat[x:]
    dfColumns = df.columns[keptFeatures]

    indexes = np.arange(len(keptFeatures))
    width = 1

    plt.bar(indexes, featureScore, width)
    plt.xticks(indexes + width * 0.50, dfColumns, rotation=90)
    plt.show()

    print(dfColumns)

    # dataStand = newData
    # i = 119 - x
    # scores.append(RandomForest(newData, target, i))
    # NoFeatures.append(50-x)

    # plt.plot(NoFeatures, scores)
    preds = RandomForest(newData, target)
    return preds


def RFaccPCA(data, target):

    dataStand = standardizeData(data)
    data2 = pd.DataFrame(dataStand)

    pca = decomposition.PCA(n_components=110)
    pca.fit(data2)
    data3 = pca.transform(data2)
    print("Explained Variance: ", np.sum(pca.explained_variance_ratio_))

    RandomForest(data3, target)


def main():
    if sys.platform == 'linux':
        filename = '../ambari_client/server_data_latest.csv'
    else:
        filename = 'H:\\Sem 2\\Project\\server_data_latest.csv'

    df = pd.read_csv(filename, na_values="None")
    df.drop(['filename', 'timestamp', 'cluster_name'], axis=1, inplace=True)
    df.dropna(inplace=True)

    df = df.convert_objects(convert_numeric=True)

    df = clusterConverge(df)

    df2 = df.as_matrix()

    data = df2[:, 1:120]
    target = df2[:, 0]

    preds = RFfeatureRed(data, target, df)
    # RandomForest(data, target)

    totalPreds = (pd.crosstab(target, preds,
                              rownames=['actual'],
                              colnames=['preds']))

    predPercent = totalPreds / totalPreds.sum(axis=1, level=0)
    print(predPercent)
    print(totalPreds)

    # seaborn.heatmap(totalPreds)

    cnf_matrix = confusion_matrix(target, preds)
    print(cnf_matrix)
    # np.set_printoptions(precision=2)

    # plot_confusion_matrix(cnf_matrix, classes=target, normalize=True,
    #                 title='Normalized confusion matrix')

    # ax = plt.subplot(111, frame_on=False)  # no visible frame
    # ax.xaxis.set_visible(False)  # hide the x axis
    # ax.yaxis.set_visible(False)

    # table(ax, totalPreds)  # where df is your data frame
    # plt.savefig('mytable.png')


if __name__ == '__main__':
    main()
