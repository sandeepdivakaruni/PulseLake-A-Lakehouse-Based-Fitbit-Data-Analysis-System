# Databricks notebook source
dbutils.widgets.text("Environment", "dev", "Set the current environment/catalog name")
env = dbutils.widgets.get("Environment")

# COMMAND ----------

# MAGIC %run ./02-setup

# COMMAND ----------

SH = SetupHelper(env)
SH.cleanup()

# COMMAND ----------

SH.setup()

# COMMAND ----------

# MAGIC %run ./03-history-loader

# COMMAND ----------

HL = HistoryLoader(env)
HL.load_history()
SH.validate()
HL.validate()

# COMMAND ----------

# MAGIC %run ./10-producer

# COMMAND ----------

PR =Producer()
PR.produce(1)

dbutils.notebook.run("./07-run", 600, {"Environment": env, "RunType": "once"})

# COMMAND ----------

PR.produce(2)
dbutils.notebook.run("./07-run", 600, {"Environment": env, "RunType": "once"})