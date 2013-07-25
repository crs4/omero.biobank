# BEGIN_COPYRIGHT
# END_COPYRIGHT

from kb_object_creator import KBObjectCreator


class EnumBase(KBObjectCreator):

    def __init__(self, name):
        super(EnumBase, self).__init__(name)
        self.enum_names = []

    def _check_enums(self):
        for ename in self.enum_names:
            enum_klass = getattr(self.kb, ename)
            enum_klass.map_enums_values(self.kb)
            for x in enum_klass.__enums__:
                self.assertEqual(x.enum_label(), x.ome_obj.value.val)
