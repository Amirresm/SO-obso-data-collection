from datetime import datetime
import csv
import logging
from util import tags_apr, tags_libs, tags_huge, tags_medium, tags_small

progressindicatorvalue = 500


def row_has_tag(row, tag_list):
    signal = False
    for tag in tag_list:
        if "<" + tag + ">" in row[12]:
            signal = True
            break
    return signal


def scan_csv(input_file_dir):
    total_rows = 578264

    tag_info = {
        'ALL': 0,
        'apr': 0,
        'libs': 0,
        'huge': 0,
        'medium': 0,
        'small': 0,
    }

    processed_rows = 0
    last_iter_time = datetime.now()
    with open(input_file_dir) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        read_line_count = 0
        for row in csv_reader:
            if read_line_count == 0:
                read_line_count += 1
            else:
                if (processed_rows % progressindicatorvalue == 0):
                    elapsed_time = datetime.now() - last_iter_time
                    last_iter_time = datetime.now()
                    estimated_time = elapsed_time * \
                        ((total_rows - processed_rows) /
                         progressindicatorvalue)
                    logging.info("   Processed %s rows. elapsed: %s ETA: %s, done: %s%%",
                                 processed_rows, elapsed_time, estimated_time, (processed_rows / total_rows) * 100)
                processed_rows += 1

                if row[3] != '1':
                    continue

                tag_info['ALL'] += 1
                if row_has_tag(row, tags_apr):
                    tag_info['apr'] += 1
                if row_has_tag(row, tags_libs):
                    tag_info['libs'] += 1
                if row_has_tag(row, tags_huge):
                    tag_info['huge'] += 1
                if row_has_tag(row, tags_medium):
                    tag_info['medium'] += 1
                if row_has_tag(row, tags_small):
                    tag_info['small'] += 1

                read_line_count += 1
        print(f'Processed {read_line_count} lines.')
    return tag_info


print('Start scanning...')

dict = scan_csv(
    "./data/intermediary/2023-01-02_01:16:08/posts.csv")

print(dict)
