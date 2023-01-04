from lxml import etree
from datetime import datetime
import csv
import logging
import argparse
from util import tags_huge, keyword_list, cols_base, wrap_tags
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("-p", "--progressindicatorvalue",
                    help="Shows nr of rows imported for larger files", type=int, default=10000000)
args = parser.parse_args()


inputdir = "/home/amirrezaesmaeili/Downloads/Persepolis/Compressed/data/Stackoverflow.com-Posts/Posts.xml"
outputdir = "./data/intermediary/" + \
    datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
Path(outputdir).mkdir(parents=True, exist_ok=True)

cols = cols_base

deprecate_keywords = keyword_list

target_tags = wrap_tags(tags_huge)

total_rows = 57721548
process_row_limit = 5000000000000
create_row_limit = 5000000000000
# process_row_limit = 50000
# create_row_limit = 500
skip_title_body_check = False


def clean_data(column, type):
    _column = column
    if str(column) == "False":
        _column = 0
    elif str(column) == "True":
        _column = 1

    if type == "CreationDate" or type == "LastActivityDate" or type == "LastEditDate" or type == "LastAccessDate":
        _column = datetime.strptime(
            _column, "%Y-%m-%dT%H:%M:%S.%f").strftime("%Y-%m-%d %H:%M:%S")
    return _column


question_cache = {}


def process_row(mode, id, parent_id, post_type_id, tags, body, title):
    if post_type_id == '1':
        postType = 'Q'
    elif post_type_id == '2':
        postType = 'A'
    else:
        postType = 'U'

    if postType == 'U':
        return False
    if mode == 'scan' and postType == 'A':
        return False

    if postType == 'Q':
        tagsOk = False
        for tag in target_tags:
            if tag in tags:
                tagsOk = True
                break
        if not tagsOk:
            return False

        if mode == 'scan':
            question_cache[id] = []

    if postType == 'A':
        bodyOk = skip_title_body_check or False
        if not skip_title_body_check:
            for keyword in deprecate_keywords:
                if keyword in body:
                    bodyOk = True
                    break
        if not bodyOk:
            return False

        if parent_id in question_cache:
            question_cache[parent_id].append(id)
        else:
            return False

    return True


def parse_xml(sourcefilename, destinationfilename, columns, mode):
    context = etree.iterparse(sourcefilename, events=('end',), tag='row')
    if mode == 'write':
        f = open(destinationfilename, 'w', newline='', encoding="utf-8")
        w = csv.writer(f, quoting=csv.QUOTE_ALL)
    processed_rows = 0
    created_rows = 0
    q_created_rows = 0
    a_created_rows = 0
    u_created_rows = 0
    if mode == 'write':
        w.writerow(columns)
    last_iter_time = datetime.now()
    for event, element in context:
        if (processed_rows % args.progressindicatorvalue == 0):
            elapsed_time = datetime.now() - last_iter_time
            last_iter_time = datetime.now()
            estimated_time = elapsed_time * \
                ((total_rows - processed_rows) / args.progressindicatorvalue)
            logging.info("   MODE: %s,Processed %s rows, created %s rows. elapsed: %s ETA: %s, done: %s%%",
                         mode, processed_rows, created_rows, elapsed_time, estimated_time, (processed_rows / total_rows) * 100)
        if processed_rows > process_row_limit or created_rows > create_row_limit:
            break

        processed_rows += 1
        post_type_id = element.attrib["PostTypeId"]
        title = (element.attrib["Title"]
                 if "Title" in element.attrib else '').strip().lower()
        body = (element.attrib["Body"]
                if "Body" in element.attrib else '').strip().lower()
        tags = (element.attrib["Tags"]
                if "Tags" in element.attrib else '').strip().lower()
        id = (element.attrib["Id"]
              if "Id" in element.attrib else '').strip().lower()
        parent_id = (element.attrib["ParentId"]
                     if "ParentId" in element.attrib else '').strip().lower()
        if process_row(mode, id, parent_id, post_type_id, tags, body, title):
            if mode == 'write':
                row = [clean_data(
                    element.attrib[column], column) if column in element.attrib else '' for column in columns]
                w.writerow(row)
                created_rows += 1
                if post_type_id == '1':
                    q_created_rows += 1
                elif post_type_id == '2':
                    a_created_rows += 1
                else:
                    u_created_rows += 1

            while element.getprevious() is not None:
                del element.getparent()[0]
        else:
            while element.getprevious() is not None:
                del element.getparent()[0]

    if mode == 'write':
        f.close()
    return processed_rows, created_rows, q_created_rows, a_created_rows, u_created_rows


format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO,
                    datefmt="%H:%M:%S")

logging.info("Starting processing")

start_time = datetime.now()
processed_rows, created_rows, q, a, u = parse_xml(
    inputdir, outputdir + "/posts.csv", cols, 'scan')
elapsed_time = datetime.now() - start_time

logging.info("SCAN: Finished processing, processed %s rows, created %s rows in %s",
             processed_rows, created_rows, elapsed_time)

start_time = datetime.now()
processed_rows, created_rows, q_created_rows, a_created_rows, u_created_rows = parse_xml(
    inputdir, outputdir + "/posts.csv", cols, 'write')
elapsed_time = datetime.now() - start_time

logging.info("WRITE: Finished processing, processed %s rows, created %s Q rows and %s A rows, Total: %s, in %s",
             processed_rows, q_created_rows, a_created_rows, created_rows, elapsed_time)

f = open(outputdir + "/relations.csv", 'w', newline='', encoding="utf-8")
w = csv.writer(f, quoting=csv.QUOTE_ALL)
w.writerow(['QuestionId', 'AnswerId'])
processed_rels = 0
created_rels = 0
created_rels_a = 0
for key, value in question_cache.items():
    processed_rels += 1
    if len(value) > 0:
        created_rels += 1
        ids = []
        for v in value:
            created_rels_a += 1
            ids.append(v)
        w.writerow([key, '-'.join(ids)])


logging.info("RELATIONS: Finished processing, processed %s rows, created %s, containing %s answers",
             processed_rels, created_rels, created_rels_a)

print(sum([len(x) for x in question_cache.values()]))
