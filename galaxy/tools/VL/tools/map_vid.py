import sys
from bl.vl.app.kb_query.main import main as kb_query

def main(argv):
  selected_column, new_column_name, input_file = argv[:3]
  selected_column = int(selected_column) - 1
  new_column_name = new_column_name.strip()

  with open(input_file) as f:
    l = f.readline().strip()
  column_names = l.split('\t')
  column_name = column_names[selected_column]

  argv = argv[3:] + ['--column=%s,%s' % (column_name, new_column_name)]
  kb_query(argv)

main(sys.argv[1:])
