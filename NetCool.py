import os
import bisect
import sys
import json
import csv
import time
import datetime
import glob
import memdb

try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    from pyspark.sql import SQLContext, Row
except ImportError as e:
    print ("error importing spark modules", e)
    sys.exit(1)

IMSI_PREVSLI_MODEL = "imsi_prevsli_db"

input_directory = "/mapr/cluster-uscc/CONFIGURATION_OUTPUT_DATA/NETCOOL/*.csv"
cell_location_mapper_file = "/mapr/cluster-uscc/netcool_test/cellular_location_mapper.txt"
directory = "/mapr/cluster-uscc/eea_data/input/esr"
time_to_look = 15
not_found_percent = 35
found_impact_1 = 25
found_impact_2 = 20
found_impact_3 = 15
output_file = "/mapr/cluster-uscc/netcool_test/"

conf = SparkConf().setAppName("Netcool project")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


# Function that takes mapping of cellular_name with location identifier
def input_cellular_mapper(filename):
    data = {}
    try:
        with open(filename, 'rU') as infile:
            # read the file as a dictionary for each row ({header : value})
            reader = csv.DictReader(infile, fieldnames=range(29))
            for row in reader:
                for header, value in row.items():
                    try:
                        data[header].append(value)
                    except KeyError:
                        data[header] = [value]
    except IOError as ie:
        print ie, "Error in reading cellular mapper file"

    cell_location_mapper = dict(zip(data[0], data[2]))
    return cell_location_mapper


def extract_timestamps(directory, outage_timestamp, outage_end_timestamp, files_to_look=3):
    def find_lt(a, x):
        'Find rightmost value less than x'
        i = bisect.bisect_left(a, x)
        if a[i - 1]:
            return i - 1
        raise ValueError

    def find_gt(a, x):
        'Find leftmost value greater than x'
        i = bisect.bisect_right(a, x)
        if a[i]:
            return i
        raise ValueError

    list_of_files = [f for f in os.listdir(directory)]
    files_with_timestamp = sorted(map(lambda x: int(x.split("=")[1]), list_of_files))

    # calculating the indexes needed to look in the timestamp
    index_prev, index_forw, after_outage_index = 0, 0, 0
    try:
        index_prev = find_lt(files_with_timestamp, int(outage_timestamp))
        index_forw = find_gt(files_with_timestamp, int(outage_timestamp))
        after_outage_index = find_lt(files_with_timestamp, int(outage_end_timestamp))
    except ValueError as V:
        print V, "Problem in indexing the directory"

    start_index_prev = (index_prev) - (files_to_look - 1)
    end_index_prev = index_prev + 1

    start_index_forw = index_forw
    end_index_forw = index_forw + (files_to_look)

    # Getting the timestamps that are needed to be looked
    tolook_timestamps = []
    tolook_timestamps.extend(files_with_timestamp[start_index_prev:end_index_prev])
    tolook_timestamps.extend(files_with_timestamp[start_index_forw:end_index_forw])
    tolook_timestamps.extend(files_with_timestamp[after_outage_index:after_outage_index + 15])

    # Appending the ts to files again
    filenames = map(lambda x: "ts=" + str(x), tolook_timestamps)
    return tuple(set(filenames))


def read_directory(filenames):
    # Just extracing the files needed
    list_of_files = []
    for name in filenames:
        list_of_files.extend([directory + "/" + name + "/" + f for f in os.listdir(directory + "/" + name)
                              if "SUCCESS" not in f])
    return list_of_files


def convert_date_to_timestamp(date_string):
    date_strip = ""
    # First try if the format is %m/%d/%Y %H:%M if not try the other format
    try:
        date_strip = datetime.datetime.strptime(date_string, "%m/%d/%Y %H:%M")
    except:
        try:
            date_strip = datetime.datetime.strptime(date_string, "%m/%d/%Y %H:%M:%S")
        except ValueError as ve:
            print ("Cannot acess the format of the data", ve)

    time_stamp = time.mktime(date_strip.timetuple())
    return time_stamp


def get_yesterday_date():
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(20)
    return time.mktime(yesterday.timetuple())


def input_netcool(filename):
    cell_with_problems = []
    try:
        with open(filename, 'rU') as infile:
            # read the file as a dictionary for each row ({header : value})
            reader = csv.reader(infile)
            timestamp_yesterday = get_yesterday_date()
            reader.next()
            for row in reader:
                try:
                    # Acessing the required columns in the row
                    start_time = row[2]
                    site_id = row[1]
                    end_time = row[3]
                    ticket_impact = row[7]
                    alaram_impact = row[16]
                    technology = row[14]
                    isaffected = row[16]
                except:
                    print "Row is not in proper format in the netcool csv file. The current row is not being processed"
                    continue
                timestamp_start = convert_date_to_timestamp(start_time)
                # This checks whether the row have service effect and timestamp is of yesterday
                if (isaffected == "SA" and timestamp_start > timestamp_yesterday):

                    # Converting the date to timestamp
                    outage_end_timestamp = convert_date_to_timestamp(end_time)

                    # Seeing the proper technology in the row
                    if (technology == "CDMA"):
                        cell_with_problems.append(
                            (timestamp_start, site_id, outage_end_timestamp, ticket_impact, alaram_impact, "2G"))
                    elif (technology == "4G"):
                        cell_with_problems.append(
                            (timestamp_start, site_id, outage_end_timestamp, ticket_impact, alaram_impact, "4G"))
                    else:
                        cell_with_problems.append(
                            (timestamp_start, site_id, outage_end_timestamp, ticket_impact, alaram_impact, "2G"))
                else:

                    print ("The row is either NSA or is of not current date")

    except ImportError as ie:
        print ie, "Cannot read the file"
    # Returning only cells with problems
    return set(cell_with_problems)


cell_location_mapper = input_cellular_mapper(cell_location_mapper_file)


def json_extract(x):
    data = {}
    try:
        data = json.loads(x)
    except:
        print "json loading failed"
        pass
    location_dict = data["loc_history"]["3gpp"]
    imsi = data["imsi"]
    timestamp = int(str(data["start"])[:10])

    location_history = []
    for key in location_dict:
        location_timestamp = int(str(location_dict[key][0])[:10])
        cell_location = None
        try:
            cell_location = cell_location_mapper[location_dict[key][2]]
        except:
            pass
        if (timestamp > location_timestamp):
            location_history.append((timestamp, imsi, cell_location, location_dict[key][1]))
        else:
            location_history.append((location_timestamp, imsi, cell_location, location_dict[key][1]))

    return location_history


def spark_process(files_list, prob_timestamp, outage_end_timestamp, location, rat, duration_to_consider):
    before_duration = int(prob_timestamp) - int(duration_to_consider)
    after_duration = int(prob_timestamp) + int(duration_to_consider)
    outage_end_timestamp = int(outage_end_timestamp)
    outage_end_duration_end = int(outage_end_timestamp) + 4500

    # This part of the code creates dataframe
    lines = sc.textFile(",".join(files_list))
    parts = lines.map(lambda l: l.split("\t"))
    json_data_extract = parts.flatMap(lambda x: (json_extract(x[3])))
    row_data = json_data_extract.map(
        lambda x: Row(timestamp=int(x[0]), imsi=str(x[1]), location=str(x[2]), rat=str(x[3])))
    schemadata = sqlContext.createDataFrame(row_data)
    schemadata.registerTempTable("JsonData")
    schemadata.cache()

    # Queries for the affected imsis
    imsi = sqlContext.sql(
        "SELECT imsi from JsonData WHERE (timestamp > " + str(before_duration) + " AND timestamp < " + str(
            after_duration) +
        ")" + " AND location LIKE '%" + str(location) + "%'")
    imsiNames = imsi.rdd.map(lambda p: str(p.imsi))
    imsi_list = imsiNames.collect()

    # queries for finding before location
    imsi_before_location = []
    if (len(imsi_list) > 0):
        cell_id = sqlContext.sql(
            "SELECT location,imsi,timestamp from JsonData WHERE (timestamp >" + str(before_duration) + \
            " AND timestamp < " + str(int(prob_timestamp)) + ") AND imsi IN " + str(tuple(imsi_list)))
        cell_location_list = cell_id.map(lambda p: (p.location, p.imsi, p.timestamp))
        imsi_before_location = cell_location_list.collect()

    # queries for finding after location
    imsi_after_location = []
    if (len(imsi_list) > 0):
        cell_id = sqlContext.sql(
            "SELECT location,imsi,timestamp from JsonData WHERE (timestamp >" + str(outage_end_timestamp) + \
            " AND timestamp < " + str(outage_end_duration_end) + ") AND imsi IN " + str(tuple(imsi_list)))
        cell_location_list = cell_id.map(lambda p: (p.location, p.imsi, p.timestamp))
        imsi_after_location = cell_location_list.collect()

    return (imsi_list, imsi_after_location, imsi_before_location)


def create_imsi_location_dict(imsi_location_list):
    # creating imsi and location dictionary
    dict_imsi = {}
    for element in imsi_location_list:

        if element in dict_imsi:
            dict_imsi[element[1]].append((element[0], element[2]))
        else:
            dict_imsi[element[1]] = [(element[0], element[2])]

    return dict_imsi


def calculate_sli(imsi_list, imsi_location_list, outage_end, ticket_impact=1):
    dict_imsi = {}
    dict_imsi = create_imsi_location_dict(imsi_location_list)
    print dict_imsi
    sli_update = []
    for imsi in imsi_list:
        # This means that imsi has travelled to other locations
        if (imsi in dict_imsi):
            location_list_imsi = dict_imsi[imsi]
            location_list_imsi = sorted(location_list_imsi, key=lambda x: x[1])
            # The timestamp when we have seen him again
            timestamp_seen = location_list_imsi[0][1]
            time_elapsed = timestamp_seen - outage_end
            # This means that the user has been seen in some other cell after the outage
            if (time_elapsed <= 900):
                if (ticket_impact == 1):
                    sli_update.append((imsi, (float(100 - found_impact_1) / 100.0)))
                elif (ticket_impact == 2):
                    sli_update.append((imsi, (float(100 - found_impact_2) / 100.0)))
                else:
                    sli_update.append((imsi, (float(100 - found_impact_3) / 100.0)))
            else:
                no_of_15mins_missed = int(time_elapsed / 15)
                if (ticket_impact == 1):
                    x = (float(100 - not_found_percent) / 100.0)
                    for index in range(no_of_15mins_missed):
                        x = x * 0.95
                    sli_update.append((imsi, x))
                else:
                    sli_update.append((imsi, (float(100 - not_found_percent) / 100.0)))

        else:
            sli_update.append((imsi, 0.5))

    return sli_update


def update_redis(sli_update, db_name):
    _db_conn = memdb.MemDB(db_name)
    new_slis = []
    for each_sli_update in sli_update:
        key = each_sli_update[0]
        json_string = _db_conn.get(key)
        data = {}
        try:
            data = json.loads(json_string)
        except:
            continue
        prev_sli = data["prevSlis"]["Global"][0]
        if (prev_sli):
            updated_sli = float(prev_sli) * each_sli_update[1]
            new_slis.append((key, prev_sli, updated_sli))
            data["prevSlis"]["Global"][0] = updated_sli
            # _db_conn.set(key,json.dumps(data))

    return new_slis


def write_to_file(output_file, new_slis, after_location, before_location, start, end):
    imsi_before_location_mapper = create_imsi_location_dict(before_location)
    imsi_after_location_mapper = create_imsi_location_dict(after_location)
    for imsi, prev_sli, update_sli in new_slis:
        imsi_before_location_string = ""
        imsi_after_location_string = ""
        print imsi, prev_sli, update_sli
        if (imsi in imsi_before_location_mapper):
            imsi_before_location_string = str(imsi_before_location_mapper[imsi])
        if (imsi in imsi_after_location_mapper):
            imsi_after_location_string = str(imsi_after_location_mapper[imsi])
        output_file.write(
            str(imsi) + "," + str(prev_sli) + "," + str(update_sli) + "," + str(start) + "," + str(end) + "," +
            imsi_before_location_string + "," + imsi_after_location_string + "\n")

def read_today_netcool_file():
    # Reading the max file in Netcool folder
    netcool_filename = max(glob.iglob(input_directory), key=os.path.getctime)
    return netcool_filename

def read_config():
    # Reading config file
    global input_directory,cell_location_mapper_file,directory,time_to_look,found_impact_1,found_impact_2,found_impact_3,output_file
    with open("config.txt") as config_file:
        
        input_directory = str(config_file.readline()[:-1].split(":")[1])
        cell_location_mapper_file = str(config_file.readline()[:-1].split(":")[1])
        directory = str(config_file.readline()[:-1].split(":")[1])
        time_to_look = int(config_file.readline()[:-1].split(":")[1]) 
        found_impact_1 = int(config_file.readline()[:-1].split(":")[1])
        found_impact_2 = int(config_file.readline()[:-1].split(":")[1])
        found_impact_3 =  int(config_file.readline()[:-1].split(":")[1])
        output_file =  str(config_file.readline()[:-1].split(":")[1])
            
        
if __name__ == "__main__":
    read_config()
    input_file = read_today_netcool_file()
    cells_to_check = input_netcool(input_file)
    outfile = open(output_file+"_"+str(time.time()), "w+")
    for query in cells_to_check:
        outage_start = query[0]
        site_id = query[1]
        outage_end_timestamp = query[2]
        ticket_impact = query[3]
        alaram_impact = query[4]
        rat = query[5]
        file_names = extract_timestamps(directory, outage_start, outage_end_timestamp,
                                        files_to_look=int(time_to_look / 5))
        files_to_read = read_directory(file_names)
        imsi_list, imsi_location, imsi_before_location = spark_process(files_to_read, outage_start,
                                                                       outage_end_timestamp, site_id, rat,
                                                                       int(time_to_look / 5) * 300)
        sli_update = calculate_sli(imsi_list, imsi_location, outage_end_timestamp, ticket_impact)
        new_slis = update_redis(sli_update, IMSI_PREVSLI_MODEL)
        write_to_file(outfile, new_slis, imsi_location, imsi_before_location, outage_start, outage_end_timestamp)
    outfile.close()