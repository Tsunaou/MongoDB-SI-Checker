import re
from files import *

class Result(object):

    def __init__(self) -> None:
        super().__init__()
        # Checking history for Realtime-SI at /home/young/Programs/Jepsen-Mongo-Txn/mongodb/store/mongodb wr replica-set w:majority time:180 timeout-txn:10 txn-len:4 r:majority tw:majority tr:snapshot partition/20211127T180445.000Z/history.edn
        self.dir = None
        self.type = None
        self.deployment = None
        self.runtime = 0
        self.timeout = 0
        self.txn_len = 0
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

    def set_count(self, count, ok, fail, info):
        self.count = count
        self.ok = ok
        self.fail = fail
        self.info = info

    def check(self):
        self.satisfy = self.INT and self.EXT

    def __str__(self) -> str:
        return "Checking {} {} count={}, ok={}, fail={}, info={}".format(self.type, self.satisfy, self.count, self.ok,
                                                                         self.fail,
                                                                         self.info)

def new_result_map():
    result_map = {
        "replica-set": {
            4: {
                60: [],
                120: [],
                180: [],
                240: [],
                300: [],
            },
            8: {
                60: [],
                120: [],
                180: [],
                240: [],
                300: [],
            },
            12: {
                60: [],
                120: [],
                180: [],
                240: [],
                300: [],
            }
        },
        "sharded-cluster": {
            4: {
                60: [],
                120: [],
                180: [],
                240: [],
                300: [],
                480: []
            },
            8: {
                60: [],
                120: [],
                180: [],
                240: [],
                300: [],
                480: []
            },
            12: {
                60: [],
                120: [],
                180: [],
                240: [],
                300: [],
                480: []
            }
        }

    }

    return result_map

if __name__ == '__main__':
    filename = "./output1129"
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
                result.deployment = re.findall(r'mongodb wr (.+) w:majority', line)[0]
                result.runtime = int(re.findall(r'time:(\d+)', line)[0])
                result.timeout = int(re.findall(r'timeout-txn:(\d+)', line)[0])
                result.txn_len = int(re.findall(r'txn-len:(\d+)', line)[0])
                result.id = re.findall(r'snapshot partition/(.+)/history.edn', line)[0]
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

        result.check()
        results.append(result)


    ok_result_map = new_result_map()
    unknown_resut_map = new_result_map()
    for res in results:
        if res.satisfy and not res.exception:
            ok_result_map[res.deployment][res.txn_len][res.runtime].append(res)
        else:
            unknown_resut_map[res.deployment][res.txn_len][res.runtime].append(res)

    save_obj_pickle(results, "results-1129.pickle")
    save_obj_pickle(ok_result_map, "ok_result_map-1129.pickle")
    save_obj_pickle(unknown_resut_map, "unknown_resut_map-1129.pickle")