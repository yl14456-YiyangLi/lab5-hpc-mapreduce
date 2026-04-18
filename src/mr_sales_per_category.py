from mrjob.job import MRJob
import csv

class MRSalesPerCategory(MRJob):

    def mapper(self, _, line):
        # Skip the header row
        if "user_id" in line:
            return

        # Parse the CSV line
        row = next(csv.reader([line]))
        category = row[3]          # 4th column = category

        # Emit (category, 1) for every transaction
        yield category, 1

    def reducer(self, key, values):
        # Sum all the 1s for each category
        yield key, sum(values)

if __name__ == "__main__":
    MRSalesPerCategory.run()
