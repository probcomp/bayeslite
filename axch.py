import bayeslite
from bayeslite.shell.hook import bayesdb_shell_cmd

from bayeslite.vs_metamodel import VSMetamodel

@bayesdb_shell_cmd('axch')
def install_vs_metamodel(self, _argin):
    bayeslite.bayesdb_register_metamodel(self._bdb, VSMetamodel())
