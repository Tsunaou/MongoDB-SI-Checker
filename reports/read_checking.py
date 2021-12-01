import re
from files import *
from scipy import stats


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
        },
        "wiredtiger": {
            4: {
                10: [],
                20: [],
                30: [],
                40: [],
                50: [],
                60: []
            },
            8: {
                10: [],
                20: [],
                30: [],
                40: [],
                50: [],
                60: []
            },
            12: {
                10: [],
                20: [],
                30: [],
                40: [],
                50: [],
                60: []
            }
        },

    }

    return result_map


if __name__ == '__main__':
    filename = "mongodb/1129-dell-opt/output-1129-dell-opt"
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
                    result.deployment = re.findall(r'mongodb wr (.+) w:majority', line)[0]
                    result.timeout = int(re.findall(r'timeout-txn:(\d+)', line)[0])
                else:
                    result.deployment = "wiredtiger"
                    result.timeout = 5000

                result.runtime = int(re.findall(r'time:(\d+)', line)[0])
                result.txn_len = int(re.findall(r'txn-len:(\d+)', line)[0])
                result.id = re.findall(r'/(.+)/history.edn', line)[0]
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

    ok_result_map = new_result_map()
    unknown_resut_map = new_result_map()
    ok_count = 0
    unknown_count = 0
    for res in results:
        if res.satisfy:
            ok_result_map[res.deployment][res.txn_len][res.runtime].append(res)
            ok_count = ok_count + 1
        else:
            unknown_resut_map[res.deployment][res.txn_len][res.runtime].append(res)
            unknown_count = unknown_count + 1

    # save_obj_pickle(results, "results-1129.pickle")
    # save_obj_pickle(ok_result_map, "ok_result_map-1129.pickle")
    # save_obj_pickle(unknown_resut_map, "unknown_resut_map-1129.pickle")

    # Fig. Perf
    if is_mongo:
        perf = {
            'replica-set': {
                60: {},
                120: {},
                180: {},
                240: {},
                300: {},
            },
            'sharded-cluster': {
                60: {},
                120: {},
                180: {},
                240: {},
                300: {},
                480: {}
            }
        }
    else:
        perf = {
            'wiredtiger': {
                10: {},
                20: {},
                30: {},
                40: {},
                50: {},
                60: {},
            },
        }

    # Fig avg perf
    if is_mongo:
        avg_perf = {
            'replica-set': {
                60: {},
                120: {},
                180: {},
                240: {},
                300: {},
            },
            'sharded-cluster': {
                60: {},
                120: {},
                180: {},
                240: {},
                300: {},
                480: {}
            }
        }
    else:
        avg_perf = {
            'wiredtiger': {
                10: {},
                20: {},
                30: {},
                40: {},
                50: {},
                60: {},
            },
        }

    # Fig stat
    if is_mongo:
        stat = {
            4: {
                60: {},
                120: {},
                180: {},
                240: {},
                300: {},
                480: {}
            },
            8: {
                60: {},
                120: {},
                180: {},
                240: {},
                300: {},
                480: {}
            },
            12: {
                60: {},
                120: {},
                180: {},
                240: {},
                300: {},
                480: {}
            }
        }
    else:
        stat = {
            4: {
                10: {},
                20: {},
                30: {},
                40: {},
                50: {},
                60: {},
            },
            8: {
                10: {},
                20: {},
                30: {},
                40: {},
                50: {},
                60: {},
            },
            12: {
                10: {},
                20: {},
                30: {},
                40: {},
                50: {},
                60: {},
            }
        }

    for deploy in ok_result_map.keys():
        deploy_data = ok_result_map.get(deploy)
        for txn_len in deploy_data.keys():
            txn_data = deploy_data.get(txn_len)
            for runtime in txn_data.keys():
                check_data = txn_data.get(runtime)
                all_count = []
                all_ok = []
                all_fail = []
                all_info = []
                all_time = []
                all_error = []

                for res in check_data:
                    # print(res)
                    all_count.append(res.count)
                    all_ok.append(res.ok)
                    all_fail.append(res.fail)
                    all_info.append(res.info)
                    all_time.append(res.time)
                    all_error.append(res.error)

                n = len(check_data)
                if n == 0:
                    continue
                avg_count = sum(all_count) / n
                avg_ok = sum(all_ok) / n
                avg_fail = sum(all_fail) / n
                avg_info = sum(all_info) / n
                avg_time = sum(all_time) / n
                avg_error = sum(all_error) / n
                print("------- {} {} {}----------".format(deploy, txn_len, runtime))
                # if deploy == "sharded-cluster" and runtime == 120 and txn_len == 4:
                #     print("oks: ", all_ok)
                #     print("times: ", all_time)
                #     print([round(all_time[i] / all_ok[i], 3) for i in range(n)])

                print("Check {} txns, avg_count={}, avg_ok={}, avg_info={}, avg_info={}, avg_time={}, error={}ms"
                      .format(n, int(avg_count), int(avg_ok), int(avg_fail), int(avg_info), int(avg_time),
                              round(avg_error, 3)))
                perf[deploy][runtime][txn_len] = avg_time
                # avg_per_txn_list = [round(all_time[i] / all_ok[i], 8) for i in range(n)]
                avg_per_txn = round(avg_time / avg_ok, 8)
                avg_perf[deploy][runtime][txn_len] = avg_per_txn
                print("Cost {}ms to per txn".format(avg_per_txn))

                if deploy == "wiredtiger":
                    stat[txn_len][runtime]['wt_all'] = avg_count
                    stat[txn_len][runtime]['wt_ok'] = avg_ok
                elif deploy == "replica-set":
                    stat[txn_len][runtime]['rs_all'] = avg_count
                    stat[txn_len][runtime]['rs_ok'] = avg_ok
                else:
                    stat[txn_len][runtime]['sc_all'] = avg_count
                    stat[txn_len][runtime]['sc_ok'] = avg_ok

    for deploy in perf.keys():
        print(deploy)
        deploy_stat = perf[deploy]
        for runtime in deploy_stat.keys():
            td = deploy_stat[runtime]
            print("{} {} {} {}".format(runtime, td[4] / 1000, td[8] / 1000, td[12] / 1000))

    for txn_len in stat.keys():
        print("op", txn_len)
        for runtime in stat[txn_len].keys():
            td = stat[txn_len][runtime]
            if runtime == 480:
                td['rs_all'] = 0
                td['rs_ok'] = 0
            if is_mongo:
                print("{} {} {} {} {}".format(runtime, td['rs_all'], td['rs_ok'], td['sc_all'], td['sc_ok']))
            else:
                print("{} {} {}".format(runtime, td['wt_all'], td['wt_ok']))

    print("Avg time for each checking each transaction")
    for deploy in avg_perf.keys():
        print(deploy)
        deploy_stat = avg_perf[deploy]
        for runtime in deploy_stat.keys():
            td = deploy_stat[runtime]
            print("{} {} {} {}".format(runtime, td[4], td[8], td[12]))
