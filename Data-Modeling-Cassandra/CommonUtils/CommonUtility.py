import os
import glob
import csv


class CommonUtils:

    def get_file_paths(self, directory: str):
        """
        Function to list the paths of all the files in the directory
        :param directory: parent directory of the files
        :return: list of all the paths
        """
        try:
            for root, _, _ in os.walk(directory):
                return glob.glob(os.path.join(root, "*.csv"))
        except Exception as ex:
            print("Error identifying paths for the files")
            raise ex

    def read_data(self, files: list):
        """
        Function to read the data from the given paths and store them in a array
        :param files: List of paths
        :return: array of arrays representing data in the form of rows
        """
        rows = []
        try:
            for file in files:
                with open(file, 'r', encoding='utf8', newline='') as csvfile:
                    csvreader = csv.reader(csvfile)
                    next(csvreader)
                    for row in csvreader:
                        rows.append(row)
            return rows
        except Exception as ex:
            print("Error reading the csv files")
            raise ex
