class Anonymizer:
    @staticmethod
    def drop(row, pii_list):
        new_row = []
        for cell in row:
            for word in pii_list:
                if word in cell:
                    cell = cell.replace(word, "")
            new_row.append(cell)
        return new_row

    @staticmethod
    def redact(row, pii_list):
        new_row = []
        for cell in row:
            for word in pii_list:
                if word in cell:
                    cell = cell.replace(word, "[Redacted]")
            new_row.append(cell)
        return new_row
