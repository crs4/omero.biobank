from docutils.parsers.rst.directives.misc import Include as BaseInclude
import os, tempfile

class LiterateProgrammingInclude(BaseInclude):
    """
    FIXME
    """

    def run(self):
        if self.arguments[0].startswith('/') or \
               self.arguments[0].startswith(os.sep):
            env = self.state.document.settings.env
            self.arguments[0] = os.path.join(env.srcdir, self.arguments[0][1:])
        fo = tempfile.NamedTemporaryFile()
        with open(str(self.arguments[0])) as fi:
          self.invert_blocks(fi, fo)
        self.arguments[0] = fo.name
        fo.close()
        return BaseInclude.run(self)

    def invert_blocks(self, fi, fo):
      def write_code_block(lines):
        indent = '  '
        code_block = indent + indent.join(lines)
        if not code_block.strip():
          return
        fo.write('\n.. code-block:: python\n\n')
        fo.write(code_block)
        fo.write('\n')

      in_litblock_block = False
      code_block_lines = []
      for l in fi:
        if l.startswith('""" ..'):
          write_code_block(code_block_lines)
          code_block_lines = []
          in_litblock_block = True
        elif l.startswith('"""') and in_litblock_block:
          in_litblock_block = False
        elif in_litblock_block:
          fo.write(l)
        else:
          code_block_lines.append(l)
      if code_block_lines:
        write_code_block(code_block_lines)


def setup(app):
    app.add_directive('lit-prog', LiterateProgrammingInclude)
