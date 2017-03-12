import argparse
import os
import glob
import json


def main():
    """Main function for generating a JSON library"""
    arg_parse_obj = argparse.ArgumentParser(description="Generate a JSON library by reading sql files")

    arg_parse_obj.add_argument("-d", "--directory", dest="directory", default="./", help="")
    arg_parse_obj.add_argument("-s", "--search-pattern", dest="search_pattern", default="*.sql", help="File search pattern, eg, '*.sql'")
    arg_parse_obj.add_argument("-o", "--outfile-json-filename", dest="out_json_filename", help="JSON file which will encoded")

    arg_obj = arg_parse_obj.parse_args()

    template_directory = arg_obj.directory
    file_pattern = arg_obj.search_pattern
    out_json_filename = arg_obj.out_json_filename
    full_out_json_filename = os.path.abspath(out_json_filename)

    full_search_pattern = os.path.join(template_directory, file_pattern)

    template_file_list = glob.glob(full_search_pattern)

    if not len(template_file_list):
        print("Search pattern '%s' did not select any template files" % full_search_pattern)
    else:

        template_library_dict = {}
        for template_file in template_file_list:

            full_template_filename = os.path.abspath(template_file)
            normalized_directory, template_filename = os.path.split(full_template_filename)

            template_name, ext = os.path.splitext(template_filename)

            with open(full_template_filename, "r") as f:
                template_content = f.read()

            template_library_dict[template_name] = template_content

        print("Writing file '%s'" % full_out_json_filename)
        with open(out_json_filename, "w") as fw:
            json.dump(template_library_dict, fw)


if __name__ == "__main__":
    main()
