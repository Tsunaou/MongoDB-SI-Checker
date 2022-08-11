import re
import pandas as pd


class Result(object):

    def __init__(self) -> None:
        super().__init__()
        # Checking history for Realtime-SI at /home/young/Programs/Jepsen-Mongo-Txn/mongodb/store/mongodb wr replica-set w:majority time:180 timeout-txn:10 txn-len:4 r:majority tw:majority tr:snapshot partition/20211127T180445.000Z/history.edn
        self.dir = None
        # Strong-SI, Realtime-SI or Session-SI
        self.type = None
        # replica-set or sharded-cluster
        self.deployment = None
        self.concurrency = None
        self.limit_ok = None
        self.rate = None
        self.key_dist = None
        self.key_count = None
        self.min_len = None
        self.max_len = None
        self.txn_type = None
        self.max_writes = None
        # 20220808T223922.000+0800
        self.id = None
        # [INFO] History is valid
        self.valid = False
        # [INFO] Checking INT Successfully
        self.INT = False
        # [INFO] Checking EXT Successfully
        self.EXT = False
        # Cost 33506 ms
        self.time = 0
        # History Status: [count=23886, okCount=23670, failCount=0, infoCount=216]
        self.count = 0
        self.ok = 0
        self.fail = 0
        self.info = 0
        # Output
        self.satisfy = False
        self.exception = False
        # [INFO] The error scale is 0.012124595seconds
        self.error = 0.0

    def set_count(self, count, ok, fail, info):
        self.count = count
        self.ok = ok
        self.fail = fail
        self.info = info

    def check(self):
        self.satisfy = self.INT and self.EXT

    def __str__(self) -> str:
        if self.deployment == "replica-set" or self.deployment == "sharded-cluster":
            return "Checking {} {} count={}, ok={}, fail={}, info={}, time={}" \
                .format(self.type, self.satisfy, self.count, self.ok, self.fail, self.info, self.time)
        else:
            return "Checking {} {} count={}, ok={}, fail={}, info={}, time={}, error={} ms" \
                .format(self.type, self.satisfy, self.count, self.ok, self.fail, self.info, self.time, self.error)



if __name__ == '__main__':
    filename = "mongodb/0811-replica/check.log"
    is_mongo = True
    # filename = "wiredtiger/1131-dell/output-1131-dell"
    # is_mongo = False

    with open(filename, "r") as f:
        outputs = list(map(lambda x: x.strip('\n'), f.readlines()))

    splits = []
    for i in range(len(outputs)):
        if '-------------------------' in outputs[i]:
            splits.append(i)

    results = []
    for i in range(len(splits) - 1):
        left = splits[i]
        right = splits[i + 1]
        result = Result()
        for j in range(left + 1, right):
            line = outputs[j]
            if "Checking history" in line:
                result.dir = re.findall(r'Checking history for .+ at (.+)/history.edn', line)[0]
                result.type = re.findall(r'Checking history for (.+) at', line)[0]
                if is_mongo:
                    result.deployment = re.findall(r'mongodb wr (.+) n', line)[0]
                else:
                    result.deployment = "wiredtiger"
                result.concurrency = int(re.findall(r'n:(\d+)', line)[0])
                result.limit_ok = int(re.findall(r'ok:(\d+)', line)[0])
                result.rate = int(re.findall(r'rate:(\d+)', line)[0])
                result.key_dist = re.findall(r'kdist:(.+) kcnt', line)[0]
                result.key_count = int(re.findall(r'kcnt:(\d+)', line)[0])
                result.min_len = int(re.findall(r'min-len:(\d+)', line)[0])
                result.max_len = int(re.findall(r'max-len:(\d+)', line)[0])
                result.max_writes = int(re.findall(r'max-write:(\d+)', line)[0])
                result.id = re.findall(r'/(.+)/history.edn', line)[0]

                txn_type = None
                if result.min_len == 1 and result.max_len == 4:
                    txn_type = 'short'
                elif result.min_len == 5 and result.max_len == 12:
                    txn_type = 'mid'
                elif result.min_len == 13 and result.max_len == 20:
                    txn_type = 'long'
                else:
                    txn_type = 'mix'
                result.txn_type = txn_type

            if "valid" in line:
                result.valid = True
            if line == "[INFO] Checking INT Successfully":
                result.INT = True
            if line == "[INFO] Checking EXT Successfully":
                result.EXT = True
            if "Cost" in line:
                result.time = int(line.split(' ')[1])
            if "History Status" in line:
                count = int(re.findall(r'count=(\d+)', line)[0])
                ok = int(re.findall(r'okCount=(\d+)', line)[0])
                fail = int(re.findall(r'failCount=(\d+)', line)[0])
                info = int(re.findall(r'infoCount=(\d+)', line)[0])
                result.set_count(count, ok, fail, info)
            if "Exception" in line:
                result.exception = True
            if "The error scale" in line:
                result.error = float(re.findall(r"scale is (.+)seconds", line)[0]) * 1000

        result.check()
        results.append(result)

    res_dicts = []
    for res in results:
        if res.exception:
            continue
        rd = {
            'concurrency': res.concurrency,
            'key-dist': res.key_dist,
            'key-count': res.key_count,
            'type': res.txn_type,
            'time': res.time
        }
        res_dicts.append(rd)

    df = pd.DataFrame(res_dicts)

    concurrencies = [5, 10, 25, 50, 100]
    key_dists = ['exponential', 'uniform']
    key_counts = [5, 10, 20]
    txn_types = ['short', 'mid', 'long', 'mix']

    import matplotlib.pyplot as plt
    import numpy as np

    df_map = dict()
    for key_dist in key_dists:
        for key_count in key_counts:
            plt.title('{}-{}'.format(key_dist, key_count))
            plt.grid(True)  # 显示网格
            plt.xlabel('concurrency')
            plt.ylabel('checking time(ms)')
            for txn_type in txn_types:
                title = '{}-{}-{}'.format(key_dist, key_count, txn_type)
                print(title)
                df_sub = df.loc[
                    (df['key-dist'] == key_dist) &
                    (df['key-count'] == key_count) &
                    (df['type'] == txn_type)]
                df_sub = df_sub[['concurrency', 'time']]
                df_map[title] = df_sub

                x = np.empty([10, 5])
                x[:] = np.NaN
                for i, concurrency in enumerate(concurrencies):
                    dfx = df_sub.loc[df_sub['concurrency'] == concurrency]
                    count = dfx['time'].count()
                    x[0:count, i] = dfx['time'].to_numpy()

                colmean = np.nanmean(x, axis=0)
                Indxs = np.where(np.isnan(x))
                x[Indxs] = np.take(colmean, Indxs[1])
                # print(x)

                # https://matplotlib.org/stable/gallery/statistics/boxplot.html
                boxprops = dict(linestyle='-', linewidth=2, color='darkgoldenrod')
                if txn_type == 'short':
                    boxprops = dict(linestyle='-', linewidth=2, color='blue')
                elif txn_type == 'mid':
                    boxprops = dict(linestyle='-', linewidth=2, color='green')
                elif txn_type == 'long':
                    boxprops = dict(linestyle='-', linewidth=2, color='red')
                elif txn_type == 'mix':
                    boxprops = dict(linestyle='-', linewidth=2, color='darkgoldenrod')
                plt.boxplot(x, labels=concurrencies, sym="r+", showmeans=True, boxprops=boxprops)  # 绘制箱线图

            plt.show()

