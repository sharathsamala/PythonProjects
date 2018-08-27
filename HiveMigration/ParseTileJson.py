import sys
import json
import traceback
import collections
from dateutil.relativedelta import *
from datetime import datetime


class ParseTileJson(object):

    def __init__(self):
        self.input_json = {}

    def fetch_month_list(self, min_month):

        month_list = []

        start_month = datetime.strptime(min_month, '%Y-%m')
        end_month = datetime.now().strftime("%Y-%m")
        current_month = start_month.strftime("%Y-%m")
        count = 1
        month_list.append(str(current_month))
        while True:
            if current_month == end_month:
                break
            else:
                current_month = (start_month + relativedelta(months=+count)).strftime("%Y-%m")
                count = count + 1
                month_list.append(str(current_month))

        return month_list


    def fetch_min_month_year(self, input):

        ordered_dict = collections.OrderedDict(sorted(input.items()))

        for key, val in ordered_dict.items():
            min_key = key
            break

        return min_key

    def fetch_val(self, key, month):

        try:
            return self.input_json[key][month]
        except:
            return None

    def process_data(self, input_json):

        min_month_list = []
        try:
            self.input_json = input_json
            tile_id = input_json["tileId"]

            min_month_list.append(self.fetch_min_month_year(input_json["drivingCount"]))
            min_month_list.append(self.fetch_min_month_year(input_json["walkingCount"]))
            min_month_list.append(self.fetch_min_month_year(input_json["appLaunchCount"]))
            min_month_list.sort()
            min_month = min_month_list[0]

            month_list = self.fetch_month_list(min_month)

            for month in month_list:

                app_cnt = self.fetch_val("appLaunchCount", month)
                walk_cnt = self.fetch_val("walkingCount", month)
                drive_cnt = self.fetch_val("drivingCount", month)

                if app_cnt is None and walk_cnt is None and drive_cnt is None:
                    pass
                else:
                    print str(tile_id)+"\t"+str(month)+"\t"+str(drive_cnt)+"\t"+str(walk_cnt)+"\t"+str(app_cnt)

        except:
            sys.stderr(traceback.format_exc())
            sys.exit(1)


def main():

    while True:
        line = sys.stdin.readline()
        input_dict = json.loads(line)
        pd = ParseTileJson()
        pd.process_data(input_dict)


if __name__ == '__main__':
    main()