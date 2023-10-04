from mrjob.job import MRJob

class MaxMinValue(MRJob):

    def mapper(self, _, line):
        col, row, value = line.split(",")
        value = int(value)

        # Yield column, value for max calculation in reducer
        yield ("COLUMN", col), value

        # Yield row, value for min calculation in reducer
        yield ("ROW", row), value

    def reducer(self, key, values):
        key_type, col_or_row = key

        # For columns, we want max value
        if key_type == "COLUMN":
            yield col_or_row, max(values)
        else:  # For rows, we want min value
            yield col_or_row, min(values)


if __name__ == '__main__':
    MaxMinValue.run()
