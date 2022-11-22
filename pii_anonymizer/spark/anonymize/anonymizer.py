from hashlib import sha256


class Anonymizer:
    @staticmethod
    def replace(row, replace_string, pii_list):
        new_row = []
        for cell in row:
            for word in pii_list:
                if word in cell:
                    cell = cell.replace(word, replace_string)
            new_row.append(cell)
        return new_row

    @staticmethod
    def hash(row, pii_list):
        new_row = []
        for cell in row:
            for word in pii_list:
                if word in cell:
                    cell = cell.replace(word, sha256(word.encode("utf-8")).hexdigest())
            new_row.append(cell)
        return new_row
