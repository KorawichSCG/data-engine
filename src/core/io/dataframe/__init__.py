from .pandas_obj import PandasCSVFrame


# ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
# DATA_LANDING_PATH = os.path.join(ROOT, 'data', 'landing')
# DATA_SUCCESS_PATH = os.path.join(ROOT, 'data', 'success')
#
#
# # prepare function --------------------------------------------------------------------------
# def _rows(f, chunk_size=1024, sep='|'):
#     """
#     Read a file where the row separator is '|' lazily
#     usage:
#         >> with open('big.csv') as f:
#         >>     for r in rows(f):
#         >>         process(r)
#     """
#     row = ''
#     while (chunk := f.read(chunk_size)) != '':   # End of file
#         while (index := chunk.find(sep)) != -1:     # No separator found
#             yield row + chunk[:index]
#             chunk = chunk[index + 1:]
#             row = ''
#         row += chunk
#     yield row
#
#
# # prepare function --------------------------------------------------------------------------
# def prepare_csv(file_name: str, delimiter: str = "|"):
#     with open(os.path.join(DATA_LANDING_PATH, file_name), mode="r", encoding="utf-8-sig") as read_file,\
#             open(os.path.join(DATA_SUCCESS_PATH, file_name), mode="w", encoding="utf-8-sig", newline='') as write_file:
#         # writer = csv.writer(write_file, delimiter="|", quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
#         writer = csv.writer(write_file, delimiter="|", quoting=csv.QUOTE_NONE)
#         # writer = csv.writer(write_file, delimiter="|", quoting=csv.QUOTE_ALL)
#         _row_count = 1
#         for row in _rows(read_file, sep='\n'):
#             data: list = []
#             append = data.append
#             try:
#                 row_split: list = row.split(delimiter)
#                 if row == '':
#                     continue
#                 for col in row_split:
#                     _col = col.strip().replace('"', '').replace("'", "'").replace(",", "_")
#                     # append(''.join(_ for _ in col if _.isalnum()))
#                     append(_col)
#                 writer.writerow(data)
#                 del data
#                 _row_count += 1
#             except csv.Error as err_stt:
#                 print(f'csv.Error: {err_stt}. with row value: {row!r}')
