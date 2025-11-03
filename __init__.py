"""Pacote de artefatos Databricks."""

from importlib import import_module
import sys

# Permite que os módulos internos sejam importados diretamente como
# ``utilitarios`` no workspace do Databricks.
sys.modules.setdefault("utilitarios", import_module("databricks.utilitarios"))