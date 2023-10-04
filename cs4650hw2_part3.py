
from mrjob.job import MRJob

class MaxMinValueMultipleExamples(MRJob):

    def mapper(self, _, line):
        col, row, value = line.split(',')
        value = int(value)
        # Emit column key with value and row example
        yield ("COLUMN", col), (value, row)
        # Emit row key with value and column example
        yield ("ROW", row), (value, col)

    def reducer(self, key, values):
        key_type, key_name = key
        values_list = list(values)  # Convert values to a list for multiple iterations
        examples = []

        if key_type == "COLUMN":
            # Find the maximum value for columns
            max_value = max(val for val, _ in values_list)
            examples = [example for val, example in values_list if val == max_value]
            yield key_name, {"value": max_value, "examples": examples}
        else:
            # Find the minimum value for rows
            min_value = min(val for val, _ in values_list)
            examples = [example for val, example in values_list if val == min_value]
            yield key_name, {"value": min_value, "examples": examples}

if __name__ == '__main__':
    MaxMinValueMultipleExamples.run()
