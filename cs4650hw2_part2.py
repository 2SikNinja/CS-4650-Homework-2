from mrjob.job import MRJob


class MaxMinValueWithExample(MRJob):

    def mapper(self, _, line):
        col, row, value = line.split(',')
        value = int(value)
        # Emit column key with value and row example
        yield ("COLUMN", col), (value, row)
        # Emit row key with value and column example
        yield ("ROW", row), (value, col)

    def reducer(self, key, values):
        key_type, key_name = key
        if key_type == "COLUMN":
            # Find the maximum value for columns and associated row
            max_value, example = max(values, key=lambda x: x[0])
            yield key_name, {"value": max_value, "example": example}
        else:
            # Find the minimum value for rows and associated column
            min_value, example = min(values, key=lambda x: x[0])
            yield key_name, {"value": min_value, "example": example}


if __name__ == '__main__':
    MaxMinValueWithExample.run()
