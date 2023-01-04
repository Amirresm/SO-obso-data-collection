from datetime import datetime
import csv
import logging
import argparse
from util import tags_huge, tags_apr, tags_libs, tags_libs_apr, keyword_list, cols_base, cols_out, tag_id_dict

parser = argparse.ArgumentParser()
parser.add_argument(
    "inputfile", help="Location of the StackOverFlow intermediary file")
parser.add_argument(
    "relationsfile", help="Location of the QA relations file")
parser.add_argument("-p", "--progressindicatorvalue",
                    help="Shows nr of rows imported for larger files", type=int, default=10000000)
args = parser.parse_args()

target_tags_dict = tag_id_dict

input_file_dir = args.inputfile


def scan_data():
    total_rows = 727765

    def load_relations_to_dict(filename):
        relations = {}
        with open(filename, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                relations[row[0]] = row[1]
        return relations

    def count_deprecate_keywords(text):
        count = 0
        for keyword in keyword_list:
            if keyword in text:
                count += 1
        return count

    def is_tag_in_tag_list(row):
        in_libs = False
        in_all = False
        for tag in tags_libs_apr:
            if "<" + tag + ">" in row[12]:
                in_libs = True
                break
        for tag in tags_apr:
            if "<" + tag + ">" in row[12]:
                in_all = True
                break
        return in_all, in_libs

    def parse_csv(input_file_dir, relations_dict):
        deprecated_count = 0
        processed_rows = 0
        created_rows = 0

        libs_count = 0
        others_count = 0

        libs_deprecate_count = 0
        others_deprecate_count = 0

        last_iter_time = datetime.now()
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
                    in_all, in_libs = is_tag_in_tag_list(row)
                    if not in_all:
                        continue

                    related_answer_ids = relations_dict.get(row[0], '')
                    related_answer_count = 0 if len(related_answer_ids) == 0 else len(
                        related_answer_ids.split('-'))

                    deprecate_keyword_count = count_deprecate_keywords(
                        (row[10] + ' ' + row[11]).lower())

                    if in_libs:
                        libs_count += 1
                        if deprecate_keyword_count > 0 or related_answer_count > 0:
                            deprecated_count += 1
                            libs_deprecate_count += 1
                    else:
                        others_count += 1
                        if deprecate_keyword_count > 0 or related_answer_count > 0:
                            deprecated_count += 1
                            others_deprecate_count += 1

                    created_rows += 1
                    read_line_count += 1
            print(f'Processed {read_line_count} lines.')
        return processed_rows, created_rows, libs_count, others_count, libs_deprecate_count, others_deprecate_count

    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

    logging.info("Starting processing")
    start_time = datetime.now()

    relations_dict = load_relations_to_dict(args.relationsfile)

    processed_rows, created_rows, libs_count, others_count, libs_deprecate_count, others_deprecate_count = parse_csv(
        input_file_dir, relations_dict)

    elapsed_time = datetime.now() - start_time
    logging.info("Finished processing, processed %s rows, created %s rows in %s",
                 processed_rows, created_rows, elapsed_time)

    logging.info("All libs: %s, All others: %s, Deprecated libs: %s, Deprecated others: %s",
                 libs_count, others_count, libs_deprecate_count, others_deprecate_count)


scan_data()
