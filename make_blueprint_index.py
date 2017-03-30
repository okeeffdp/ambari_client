
"""
Make an index file for the blueprints
Converts each unique configuration to a number
"""

import itertools

settings = [("dfs.replication", "hdfs-site"),
            ("yarn.nodemanager.resource.memory-mb", "yarn-site"),
            ("yarn.scheduler.minimum-allocation-mb", "yarn-site"),
            ("hive.vectorized.execution.enabled", "hive-site"),
            ("hive.vectorized.execution.reduce.enabled", "hive-site")]

dfs = ["1", "2", "3", "4", "5"]
yarn = [("2048", "682"),
        ("2048", "1024"),
        ("2816", "1280"),
        ("2816", "1664")]
hive1 = ["true", "false"]
hive2 = ["true", "false"]

combs = itertools.product(dfs, yarn, hive1, hive2)

with open("./blueprint_index.txt", "w") as f:
    f.write("index," + ",".join([i[0] for i in settings]) + "\n")
    for idx, obj in enumerate(combs):
        idx, obj = str(idx), list(obj)

        yarn_max, yarn_min = obj.pop(1)
        obj.insert(1, yarn_min)
        obj.insert(1, yarn_max)

        line = idx + "," + ",".join(obj)
        f.write(line + "\n")

