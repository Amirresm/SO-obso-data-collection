from lxml import etree
from pathlib import Path
from datetime import datetime
import csv
import logging
import argparse
from dateutil import relativedelta
from nltk.corpus import stopwords
from bs4 import BeautifulSoup
import re
from util import tags_huge, tags_apr, tags_libs, keyword_list, tags_libs_clean, cols_out, tag_id_dict, tags_libs_apr

parser = argparse.ArgumentParser()
parser.add_argument(
    "inputfile", help="Location of the StackOverFlow intermediary file")
parser.add_argument(
    "relationsfile", help="Location of the QA relations file")
parser.add_argument(
    "output", help="Export destination file")
parser.add_argument("-p", "--progressindicatorvalue",
                    help="Shows nr of rows imported for larger files", type=int, default=10000000)
args = parser.parse_args()

target_tags_dict = tag_id_dict

input_file_dir = args.inputfile


def create_dataset(out_name, target_tags):
    output_file_dir = "./data/final/" + out_name
    Path("./data/final/").mkdir(parents=True, exist_ok=True)

    # total_rows = 804663
    # total_rows = 727765
    total_rows = 578264

    def load_relations_to_dict(filename):
        relations = {}
        with open(filename, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                relations[row[0]] = row[1]
        return relations

    def remove_special_characters(text):
        return re.sub(r'[^a-zA-Z\s]', ' ', text)

    def remove_stopwords(text):
        return ' '.join([word for word in text.split() if word not in stopwords.words('english')])

    def striphtml(data):
        soup = BeautifulSoup(data, 'lxml')
        cleantext = soup.get_text()
        soup1 = BeautifulSoup(cleantext, 'html5lib')
        cleantext = soup1.get_text()
        cleantext = re.sub('<.*?>', ' ', str(cleantext))
        cleantext = re.sub('\\n', ' ', str(cleantext))
        cleantext = re.sub('\sdiv\s', ' ', str(cleantext),
                           flags=re.MULTILINE | re.DOTALL)

        return cleantext

    def clean_text(text):
        return remove_special_characters(remove_stopwords(striphtml(text)))

    def count_deprecate_keywords(text):
        count = 0
        for keyword in keyword_list:
            if keyword in text:
                count += 1
        return count

    def diff_month(s1, s2):
        date1 = datetime.strptime(s1, '%Y-%m-%d %H:%M:%S')
        date2 = datetime.strptime(s2, '%Y-%m-%d %H:%M:%S')
        r = relativedelta.relativedelta(date2, date1)
        return r.months + (12*r.years)

    def get_tag_list_from_row(row):
        tags_in_row = []
        for tag in target_tags:
            if "<" + tag + ">" in row[12]:
                tags_in_row.append(tag)
        tag_list = [str(target_tags_dict[tag]) for tag in tags_in_row]
        return tag_list

    def create_new_row(row, content, tag_string, related_answer_ids, related_answer_count):
        # is_deleted = '1' if row[6] != '' else ''
        is_accepted_related = '1' if row[1] and row[1] in related_answer_ids else ''
        creation_date = row[4][:11]
        # last_active_date = row[7][:11]
        activity_months = diff_month(row[4], row[7])
        new_row = [
            row[0],
            # row[1],
            # row[2],
            # row[3],
            creation_date,
            # last_active_date,
            activity_months,
            # is_deleted,
            tag_string,
            content,
            # related_answer_ids,
            is_accepted_related,
            related_answer_count,
            row[15],
            row[13],
            row[14],
            # row[16],
            # row[17],
        ]
        return new_row

    def parse_csv(input_file_dir, output_file_dir, relations_dict):
        deprecated_count = 0
        min_tag_len = 1000
        max_tag_len = 0

        processed_rows = 0
        created_rows = 0
        last_iter_time = datetime.now()
        with open(output_file_dir, 'w') as f:
            csv_writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            csv_writer.writerow(cols_out)

            with open(input_file_dir) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                read_line_count = 0
                for row in csv_reader:
                    if read_line_count == 0:
                        read_line_count += 1
                    else:
                        if (processed_rows % args.progressindicatorvalue == 0):
                            elapsed_time = datetime.now() - last_iter_time
                            last_iter_time = datetime.now()
                            estimated_time = elapsed_time * \
                                ((total_rows - processed_rows) /
                                 args.progressindicatorvalue)
                            logging.info("   Processed %s rows, created %s rows. elapsed: %s ETA: %s, done: %s%%",
                                         processed_rows, created_rows, elapsed_time, estimated_time, (processed_rows / total_rows) * 100)
                        processed_rows += 1

                        if row[3] != '1':
                            continue
                        tag_list = get_tag_list_from_row(row)
                        tag_list_len = len(tag_list)
                        if tag_list_len == 0:
                            continue
                        if tag_list_len < min_tag_len:
                            min_tag_len = tag_list_len
                        if tag_list_len > max_tag_len:
                            max_tag_len = tag_list_len

                        related_answer_ids = relations_dict.get(row[0], '')
                        related_answer_count = 0 if len(related_answer_ids) == 0 else len(
                            related_answer_ids.split('-'))

                        tag_text = "-".join(tag_list)
                        deprecate_keyword_count = count_deprecate_keywords(
                            (row[10] + ' ' + row[11]).lower())

                        if deprecate_keyword_count > 0 or related_answer_count > 0:
                            deprecated_count += 1

                        new_row = create_new_row(
                            row, deprecate_keyword_count, tag_text, related_answer_ids, related_answer_count)
                        csv_writer.writerow(new_row)

                        created_rows += 1
                        read_line_count += 1
                print(f'Processed {read_line_count} lines.')
        return processed_rows, created_rows, deprecated_count, min_tag_len, max_tag_len

    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

    logging.info("Starting processing for %s", out_name)
    start_time = datetime.now()

    relations_dict = load_relations_to_dict(args.relationsfile)

    processed_rows, created_rows, deprecated_count, min_tag_len, max_tag_len = parse_csv(
        input_file_dir, output_file_dir, relations_dict)

    elapsed_time = datetime.now() - start_time
    logging.info("Finished processing %s, processed %s rows, created %s rows in %s",
                 out_name, processed_rows, created_rows, elapsed_time)

    logging.info("Min tag length: %s, Max tag length: %s, Deprecated count: %s",
                 min_tag_len, max_tag_len, deprecated_count)


if args.output == 'all':
    all = [
        ('huge.csv', tags_huge),
        ('apr.csv', tags_apr),
        ('libs.csv', tags_libs),
        ('libs_apr.csv', tags_libs_apr),
        ('libs_clean.csv', tags_libs_clean),
    ]
    print('Creating all datasets \n')
    for name, tags in all:
        create_dataset(name, tags)
else:
    create_dataset('libs_clean.csv', tags_libs_clean)
