#!/usr/local/bin/python3

"""
Python script to receive a SQL file and an output path, to execute Oracle and dump query into a file with results.

Abel Abraham Munguia
abmurz@gmail.com

Versions:
1.0 - 2022-03-23 | Initial.
"""

# Import common libraries
import os
import csv
import argparse # you may need to install this with pip

class DBUnload:
    """
    Class Script to manage argument declaration/parsing and dump functions.
    """
    arguments_parser = 0
    arguments = 0
    output_delimiter = '|'
    output_line_terminator = '\n'
    output_quoting = csv.QUOTE_ALL
    output_escape_char = '\\'
    output_double_quote = True
    oracle_array_size = 1000

    def set_arguments(self, description_string):
        """
        Function to set arguments description program.
        :param description_string: Text to display
        :return: Nothing
        """
        self.arguments_parser = argparse.ArgumentParser(description=description_string)

    def add_argument(self, name, req, typ, help_arg):
        """
        Function to append argument supported by the script.

        :param name: Argument name
        :param req: True or False
        :param typ: Data type
        :param help_arg: Description
        :return: Nothing
        """
        self.arguments_parser.add_argument(name, required=req, type=typ, help=help_arg)

    def parse_arguments(self):
        """
        Function to parse all configured arguments
        :return: Nothing
        """
        self.arguments = self.arguments_parser.parse_args()

    def write_to_file(self, rows, mode, multiple):
        """
        Function to write incoming rows to output path.

        :param rows: list to be written
        :param mode: file open mode
        :param multiple: multiple results at once
        :return: None
        """

        # If optional args are valid, take them for output config
        if self.arguments.output_delimiter is not None:
            self.output_delimiter = self.arguments.output_delimiter

        if self.arguments.output_line_terminator is not None:
            self.output_line_terminator = self.arguments.output_line_terminator

        if self.arguments.output_quoting is not None:
            self.output_quoting = self.arguments.output_quoting

        if self.arguments.output_escape_char is not None:
            self.output_escape_char = self.arguments.output_escape_char

        if self.arguments.output_double_quote is not None:
            self.output_double_quote = self.arguments.output_double_quote

        # Define output format as dialect
        csv.register_dialect('your_dialect', delimiter=self.output_delimiter, lineterminator=self.output_line_terminator,
                                             quoting=self.output_quoting, escapechar=self.output_escape_char,
                                             doublequote=self.output_double_quote)

        # Open output path for writing, specify new line delimiter
        with open(self.arguments.output_file, mode, newline=self.output_line_terminator) as output_file:
            # Declare writer
            csv_writer = csv.writer(output_file, dialect="your_dialect")

            # Write row(s)
            csv_writer.writerows(rows) if multiple is True else csv_writer.writerow(rows)

    def set_oracle_optional_args(self, query):
        """
        Function to replace optional arguments in query text.

        :param query: query text
        :return: modified query with arguments applied
        """

        # if arg1 is valid, replace it
        if self.arguments.oracle_argument_1 is not None:
            query = query.replace(":oracle_argument_1", self.arguments.oracle_argument_1)

        # if arg2 is valid, replace it
        if self.arguments.oracle_argument_2 is not None:
            query = query.replace(":oracle_argument_2", self.arguments.oracle_argument_2)

        # if arg3 is valid, replace it
        if self.arguments.oracle_argument_3 is not None:
            query = query.replace(":oracle_argument_3", self.arguments.oracle_argument_3)

        # Verbose
        print("Oracle query:\n{0}".format(query))

        # return modified query
        return query

    def get_oracle_results(self):
        """
        Function to prepare/execute query and returns results.

        :return: None
        """

        # Import Oracle library
        import cx_Oracle

        # Read input file
        with open(self.arguments.sql_file, 'r') as fileInput:
            # Replace optional arguments on the fly
            query = self.set_oracle_optional_args(fileInput.read())

        # Read AIM_PSWD from environment to use that Oracle connection string
        con_string = os.environ.get('AIM_PSWD')

        # Create Oracle connection
        connection = cx_Oracle.connect(con_string)

        # Declare Oracle cursor
        cursor = connection.cursor()

        # Alter date format in Oracle session, this is optional, you may comment out next line if you want.
        cursor.execute("ALTER SESSION set NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'")

        if self.arguments.oracle_array_size is not None:
            self.oracle_array_size = self.arguments.oracle_array_size

        # cx_Oracle will fetch rows from Oracle N rows at a time,
        # reducing the number of network round trips that need to be performed
        # this also levers down memory consumption.
        cursor.arraysize = self.oracle_array_size

        # Execute prepared query
        try:
            cursor.execute(query)
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            print(error.message)
            exit(1)

        # Verbose
        print("Output file: {0}".format(self.arguments.output_file))

        # Obtain result column names
        column_names = [column[0] for column in cursor.description]

        # Write column names
        self.write_to_file(column_names, 'w', False)

        row_count = 0

        # Iterate over the results N times to avoid high memory consumption
        while True:
            rows = cursor.fetchmany(self.oracle_array_size)
            if not rows:
                break
            row_count = row_count + len(rows)
            self.write_to_file(rows, 'a', True)

        # Close Oracle cursor
        cursor.close()

        # Close Oracle connection
        connection.close()

        # Verbose
        print("Rows unloaded: {:,}".format(row_count))
        print("Output file size: {:,} bytes".format(os.path.getsize(self.arguments.output_file)))

def main():
    """
    Main function.

    :return: Common success or exit error
    """

    # Declare script object
    dbunload = DBUnload()

    # Declare arguments text
    dbunload.set_arguments("Parameters to generate CSV file:")

    # Declare mandatory query file argument
    dbunload.add_argument("--sql_file", True, str, "SQL input file")

    # Declare mandatory output folder argument
    dbunload.add_argument("--output_file", True, str, "Output file to dump data")

    # Declare optional text argument 1
    dbunload.add_argument("--oracle_argument_1", False, str, "Oracle text argument 1")

    # Declare optional text argument 2
    dbunload.add_argument("--oracle_argument_2", False, str, "Oracle text argument 2")

    # Declare optional text argument 3
    dbunload.add_argument("--oracle_argument_3", False, str, "Oracle text argument 3")

    # Declare optional
    dbunload.add_argument("--oracle_array_size", False, int, "Oracle array size")

    # Declare optional
    dbunload.add_argument("--output_delimiter", False, str, "Output format delimiter")

    # Declare optional
    dbunload.add_argument("--output_line_terminator", False, str, "Output format line terminator")

    # Declare optional
    dbunload.add_argument("--output_quoting", False, int, "Output format quoting (QUOTE_ALL = 1, QUOTE_MINIMAL = 0, QUOTE_NONE = 3, QUOTE_NONNUMERIC = 2)")

    # Declare optional
    dbunload.add_argument("--output_escape_char", False, str, "Output format escape char")

    # Declare optional
    dbunload.add_argument("--output_double_quote", False, bool, "Output format double quote (True/False)")

    # Parse configured arguments
    dbunload.parse_arguments()

    # Check if SQL input file exists
    if not os.path.isfile(dbunload.arguments.sql_file):
        print("SQL input file doesn't exist")
        exit(1)

    # Touch output file to see if path exists and we can write
    os.system("touch {0}".format(dbunload.arguments.output_file))

    # Check if touched file exists
    if not os.path.isfile(dbunload.arguments.output_file):
        print("Invalid output file path or no write permissions")
        exit(1)

    # Invoke get_oracle_results
    dbunload.get_oracle_results()

# Route script to execute main function
if __name__ == '__main__':
    # Invoke main function from this file
    main()
