import re, requests
import pandas as pd
import time
import csv
import pickle
import shelve
import sys

# from pathlib import Path (ONLY USE FOR THE "Failed stuff" comment)

t0 = time.time()
# Chunks to read in arin, looks like isn't needed
'''
chunksize = 10 ** 6
count = 0
for chunk in pd.read_csv('log.csv', chunksize=chunksize):
	#print(chunk['ip'])
	#time.sleep(1)
	count += 1
print(count)
time.sleep(5)
'''

df = pd.read_csv('data.csv')
set_of_names = df[['sp_entity_name', 'sp_address1', 'sp_address2', 'sp_zip', 'sp_location']]

if (len(sys.argv) < 2):
    print("Incorrect use of main_firstname.py, please use in the following format:")
    print("Option 1: python3 main_firstname.py w")
    print("Option 2: python3 main_firstname.py n")
    print(
        "w means you do NOT want to clear the dictionaries when starting the program (you only need to run the matching part)")
    print(
        "n means you want to run through the arin.txt matching as well as the mathcing part (ie you want to regen your dictionaries)")
    raise SystemExit

# ABSOLUTELY MUST USE SYSARG
# USAGE
name_dict = shelve.open('dict', writeback=True, flag=sys.argv[1])
oneword_dict = shelve.open('one_dict', writeback=True, flag=sys.argv[1])
orgname_dict = shelve.open('orgname_dict', writeback=True, flag=sys.argv[1])

# Failed stuff, possibly keep for later
'''
sections = list("abcdefghijklmnopqrstuvwxyz")
​
print(sections)
​
shelfPath = Path(sys.argv[0]).parent / Path(sec)
​
section_dict = {sec : shelve.open(fr'{shelfSavePath}') for sec in sections}
​
print(section_dict)
time.sleep(5)
'''

temp_list_name = []
temp_list_address = []

cur_orgID = ""
cur_org = ""
first_name = ""
second_name = ""
cur_street = ""
cur_city = ""
cur_postal = ""

punc = '''!()[]{};'"\,<>./?@#$%^&*_~'''  # for punctiation removal

banned_words = ['a', 'the']

print("STARTING:")

count = 0
num_lines = sum(1 for line in open('arin_db.txt'))  # num of lines, purely for counting percent complete

f = open("output.txt", "w")

if sys.argv[1] == 'n':

    with open('arin_db.txt', 'r') as data_file:
        # the assumption is that you are running this chunk of code to re-enter items, so the dicts must be treated as empty.

        for line in data_file:

            data = line.split(':')

            if (len(data) == 2):
                # print(data[1])

                d = data[0].strip()
                # print(d)
                # print(data[0].strip()," : " , data[1].strip())

                if d == "OrgID":
                    cur_orgID = (data[1].strip())
                elif d == "OrgName":  # Get first name and second name (first name is key)
                    d_raw = data[1].strip()
                    d_first = list(filter(None, data[1].split(" ")))
                    d_second = list(filter(None, data[1].split(" ")))

                    # print(d_first[0].strip(), ": D First")
                    first_name = re.sub(r'[^\w\s]', '', d_first[0].strip())
                    if (len(d_second) < 2):
                        second_name = ""
                    else:
                        second_name = re.sub(r'[^\w\s]', '', d_first[1].strip())
                        # print(d_first[0].strip(), d_first[1].strip())
                    # print(first_name, second_name)

                    cur_org = (data[1].strip())
                elif d == "Street":
                    cur_street = (data[1].strip())
                elif d == "PostalCode":
                    cur_postal = (data[1].strip())
                elif d == "City":
                    cur_city = (data[1].strip())

                    if ("-" in second_name):
                        print(second_name)
                        second_name = second_name.split("-", 1)
                        second_name = second_name[0]
                        print(second_name)

                    combined = first_name.lower() + " " + second_name.lower()

                    if combined not in name_dict:
                        name_dict[combined] = [cur_orgID]
                    elif len(name_dict[combined]) < 200:
                        name_dict[combined].append(cur_orgID)
                    if first_name.lower() not in banned_words:  # optimization check if not in dict, if first name is in banned words then use the second name\
                        if first_name not in oneword_dict:
                            oneword_dict[first_name] = [cur_orgID]
                        elif len(oneword_dict[first_name]) < 200:
                            oneword_dict[first_name].append(cur_orgID)
                    else:
                        if second_name not in oneword_dict:
                            oneword_dict[second_name] = [cur_orgID]
                        elif len(oneword_dict[second_name]) < 200:
                            oneword_dict[second_name].append(cur_orgID)
                    # adding orgname to account for the netID issue

                    # for testing, when WF first encountered should be False, encoutners after should be True

                    # if(lower_org == "wells fargo"):
                    # print(lower_org, cur_org, lower_org in orgname_dict)
                    # pass

                    if (count % 100000 == 0):
                        print("Cur Line:", count, "Percent: ", count / num_lines * 100, "% Done")
                        # print(first_name, second_name, combined, name_dict[combined])
            count += 1

f.close()
print(name_dict["wells fargo"])
time.sleep(5)

print("Confirming pickle retrieval")
time.sleep(0.5)
print(list(name_dict.keys())[:100])  # confirms
# ADD LATER, SAVED DICT PKL DUMPS (not compatible w/ for loop below becuase keyerrors, need to fix)

# with open('name_dict.pkl', 'wb') as f:
#    pickle.dump(name_dict, f)

# with open('name_dict.pkl', 'rb') as f:
#    loaded_dict = pickle.load(f)

temp_d = {}

# for k in name_dict.keys():
# print(k.strip())

print("Starting: ")

num_entries = len(set_of_names['sp_entity_name'])

c = 0

set_of_companies = set()
not_found = {}
for i in range(num_entries):
    c += 1
    if (not isinstance(set_of_names['sp_entity_name'][i], float)):
        names_first = (
            set_of_names['sp_entity_name'][i].strip().split(" "))  # split set_of_names by space, creating a list
        # print(names_first[0].strip())
        first = re.sub(r'[^\w\s]', '', names_first[0].strip())
        second = re.sub(r'[^\w\s]', '', names_first[1].strip())
        combined = first.lower() + " " + second.lower()
        if (combined not in set_of_companies):  # makes it faster, no duplicates allowed
            set_of_companies.add(combined)  # add to our duplicate list
            if (combined in list(name_dict.keys())):  # if the first word is in keys (dict.keys is already lower)
                # print(names_first, " FOUND : ", name_dict[combined.strip()], set_of_names['sp_zip'][i])
                if (set_of_names['sp_entity_name'][i] not in temp_d.keys()):
                    temp_d[set_of_names['sp_entity_name'][i]] = name_dict[
                        combined]  # Removed ,set_of_names['sp_zip'][i]
            else:
                if first in oneword_dict:
                    # print("Not Found: ", set_of_names['sp_entity_name'][i])

                    not_found[set_of_names['sp_entity_name'][i]] = oneword_dict[first]

        if c % 1000 == 0:
            print(c / num_entries * 100, "% Done")

# Part to do the not_found companies


l = []

c = open("combined.csv", "w")
cwrite = csv.writer(c)
cwrite.writerow(['Fund Name', 'Company Name', 'orgID'])

with open("two_word_match.csv", "w") as f:
    write = csv.writer(f)
    write.writerow(['Fund Name', 'count', 'orgID'])
    for k, v in temp_d.items():
        l.append("".join(k))
        l.append(len(v))
        l.append(",".join(v))
        write.writerow(l)
        l = []

with open("one_word_match.csv", "w") as f:
    write = csv.writer(f)
    write.writerow(['Fund Name', 'count', 'orgID'])
    for k, v in not_found.items():
        l.append("".join(k))
        l.append(len(v))
        l.append(",".join(v))
        write.writerow(l)
        l = []
f.close()

print("Closing files, please wait (may take a few mins, please leave this open and do something else)")
name_dict.close()
oneword_dict.close()
orgname_dict.close()
print("Done, exiting program")
